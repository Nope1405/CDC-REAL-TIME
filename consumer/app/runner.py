"""Consume -> batch -> bulk index -> commit loop.

Delivery guarantee: at-least-once. Offsets are committed only after every
record of the batch is either acknowledged by Elasticsearch or acknowledged
by the DLQ topic. Idempotent writes (doc id = primary key, external version)
make the resulting duplicates harmless.
"""

from __future__ import annotations

import time
from typing import Any, Protocol

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition

from . import metrics
from .config import Settings
from .events import EventError, IndexAction, RawMessage, coalesce, parse
from .health import Liveness
from .sink import EsSink, RetryDeadlineExceeded, ShutdownRequested

log = structlog.get_logger(__name__)


class ConsumerLike(Protocol):
    def subscribe(self, topics: list[str], **kwargs: Any) -> None: ...
    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Message]: ...
    def poll(self, timeout: float = -1) -> Message | None: ...
    def commit(self, **kwargs: Any) -> Any: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def assignment(self) -> list[TopicPartition]: ...
    def close(self) -> None: ...


def to_raw(msg: Message) -> RawMessage:
    ts_type, ts = msg.timestamp()
    return RawMessage(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        key=msg.key(),
        value=msg.value(),
        headers={k: (v or b"") for k, v in (msg.headers() or [])},
        timestamp_ms=ts if ts_type != 0 else None,  # 0 = TIMESTAMP_NOT_AVAILABLE
    )


class CdcConsumer:
    def __init__(self, settings: Settings, consumer: ConsumerLike, sink: EsSink,
                 dlq: Any, liveness: Liveness) -> None:
        self._s = settings
        self._consumer = consumer
        self._sink = sink
        self._dlq = dlq
        self._liveness = liveness
        self._topics = settings.topic_entities()
        self._buffer: list[RawMessage] = []
        self._batch_started = 0.0
        self._running = True
        self._flushing = False
        self._in_rebalance = False
        self._paused: list[TopicPartition] = []

    # ------------------------------------------------------------------ control
    def stop(self, *_: Any) -> None:
        log.info("shutdown_requested")
        self._running = False

    def run(self) -> None:
        self._consumer.subscribe(list(self._topics), on_assign=self._on_assign,
                                 on_revoke=self._on_revoke, on_lost=self._on_lost)
        log.info("subscribed", topics=list(self._topics), group=self._s.kafka_group_id)
        flush_interval = self._s.flush_interval_ms / 1000
        try:
            while self._running:
                self._liveness.beat()
                room = self._s.batch_size - len(self._buffer)
                wait = flush_interval
                if self._buffer:
                    wait = max(0.0, flush_interval - (time.monotonic() - self._batch_started))
                for msg in self._consumer.consume(num_messages=max(room, 1), timeout=wait):
                    if msg.error():
                        self._handle_kafka_error(msg.error())
                        continue
                    if not self._buffer:
                        self._batch_started = time.monotonic()
                    self._buffer.append(to_raw(msg))

                full = len(self._buffer) >= self._s.batch_size
                due = self._buffer and time.monotonic() - self._batch_started >= flush_interval
                if full or due:
                    self.flush()
            # graceful shutdown: drain what we have, then commit
            if self._buffer:
                self.flush()
        except ShutdownRequested:
            log.warning("shutdown_during_retry_batch_not_committed", buffered=len(self._buffer))
        finally:
            self._consumer.close()   # leaves the group => immediate rebalance
            log.info("consumer_closed")

    @staticmethod
    def _handle_kafka_error(err: KafkaError) -> None:
        if err.code() == KafkaError._PARTITION_EOF:
            return
        if err.fatal():
            raise KafkaException(err)
        log.warning("kafka_error", error=str(err))

    # ------------------------------------------------------------------ flush
    def flush(self, *, deadline: float | None = None) -> None:
        if not self._buffer:
            return
        self._flushing = True
        batch, self._buffer = self._buffer, []
        try:
            self._process(batch, deadline=deadline)
        finally:
            self._flushing = False

    def _process(self, batch: list[RawMessage], *, deadline: float | None) -> None:
        metrics.BATCH_SIZE.observe(len(batch))
        now_ms = int(time.time() * 1000)
        actions: list[IndexAction] = []
        dead: list[tuple[str, RawMessage, str, str]] = []   # entity, msg, reason, error

        for msg in batch:
            entity = self._topics.get(msg.topic)
            if entity is None:
                continue
            try:
                action = parse(entity, msg, now_ms=now_ms, toast_placeholder=self._s.toast_placeholder)
            except EventError as exc:
                dead.append((entity, msg, "parse_error", str(exc)))
                continue
            metrics.EVENTS_CONSUMED.labels(entity, action.cdc.get("op", action.kind.value)).inc()
            actions.append(action)

        merged = coalesce(actions)
        for entity in {a.entity for a in actions}:
            dropped = sum(a.entity == entity for a in actions) - sum(a.entity == entity for a in merged)
            if dropped:
                metrics.COALESCED.labels(entity).inc(dropped)

        if merged:
            in_callback = self._in_rebalance
            result = self._sink.write(
                merged,
                should_stop=lambda: not self._running and not in_callback,
                on_wait=None if in_callback else self._wait_paused,
                on_recover=None if in_callback else self._resume,
                deadline=deadline,
            )
            for action, error in result.failed:
                dead.append((action.entity, action.message, "es_rejected", error))
            self._observe_lag(merged, result.indexed_at_ms, {id(a) for a, _ in result.failed})

        for entity, msg, reason, error in dead:
            self._dlq.publish(entity, msg, reason, error)
        if dead:
            self._dlq.flush()

        self._commit(batch)

    def _commit(self, batch: list[RawMessage]) -> None:
        last: dict[tuple[str, int], int] = {}
        for msg in batch:
            key = (msg.topic, msg.partition)
            last[key] = max(last.get(key, -1), msg.offset)
        assigned = {(tp.topic, tp.partition) for tp in self._consumer.assignment()}
        offsets = [TopicPartition(t, p, o + 1) for (t, p), o in last.items() if (t, p) in assigned]
        if not offsets:
            return
        try:
            self._consumer.commit(offsets=offsets, asynchronous=False)
            metrics.COMMITS.labels("ok").inc()
        except KafkaException as exc:
            # Typically a rebalance raced us: the new owner re-reads and re-indexes
            # (idempotent), so this is safe to log and continue.
            metrics.COMMITS.labels("failed").inc()
            log.warning("commit_failed", error=str(exc))

    def _observe_lag(self, actions: list[IndexAction], done_ms: int, failed: set[int]) -> None:
        for a in actions:
            if id(a) in failed:
                continue
            cdc = a.cdc
            src, conn, kafka, consumed = (cdc.get(k) for k in ("source_ts", "connector_ts", "kafka_ts", "consumed_at"))
            stages = {
                "source": (src, conn), "kafka": (conn, kafka),
                "consume": (kafka, consumed), "index": (consumed, done_ms), "e2e": (src, done_ms),
            }
            for stage, (start, end) in stages.items():
                if start is not None and end is not None:
                    metrics.LAG.labels(a.entity, stage).observe(max(0, end - start) / 1000)
            if src is not None:
                metrics.LAST_SOURCE_TS.labels(a.entity).set(src / 1000)

    # ------------------------------------------------------------------ backpressure
    def _wait_paused(self) -> None:
        """Called during ES backoff: pause fetching but keep polling (group heartbeat,
        max.poll.interval.ms, rebalance callbacks)."""
        if not self._paused:
            self._paused = list(self._consumer.assignment())
            if self._paused:
                self._consumer.pause(self._paused)
                metrics.PAUSED.set(1)
                log.warning("partitions_paused", count=len(self._paused))
        self._liveness.beat()
        msg = self._consumer.poll(0)
        if msg is not None and not msg.error():
            # Only partitions assigned after the pause can deliver here; keep the record.
            if not self._buffer:
                self._batch_started = time.monotonic()
            self._buffer.append(to_raw(msg))

    def _resume(self) -> None:
        if not self._paused:
            return
        assigned = {(tp.topic, tp.partition) for tp in self._consumer.assignment()}
        still = [tp for tp in self._paused if (tp.topic, tp.partition) in assigned]
        if still:
            self._consumer.resume(still)
        self._paused = []
        metrics.PAUSED.set(0)
        log.info("partitions_resumed", count=len(still))

    # ------------------------------------------------------------------ rebalance
    def _on_assign(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        metrics.ASSIGNED.set(len(partitions))
        log.info("partitions_assigned", partitions=[f"{p.topic}[{p.partition}]" for p in partitions])

    def _on_revoke(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        log.info("partitions_revoked", partitions=[f"{p.topic}[{p.partition}]" for p in partitions])
        if self._flushing:
            # Revoked while a flush is retrying: that flush's commit will skip
            # partitions we no longer own. Nothing else to do here.
            return
        self._in_rebalance = True
        try:
            self.flush(deadline=time.monotonic() + self._s.revoke_flush_timeout_s)
        except (RetryDeadlineExceeded, ShutdownRequested):
            log.warning("revoke_flush_abandoned", note="records will be re-consumed by the new owner")
        finally:
            self._in_rebalance = False
        self._drop_buffered(partitions)
        self._paused = []
        metrics.PAUSED.set(0)
        metrics.ASSIGNED.set(0)

    def _on_lost(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        log.warning("partitions_lost", count=len(partitions))
        self._drop_buffered(partitions)

    def _drop_buffered(self, partitions: list[TopicPartition]) -> None:
        gone = {(p.topic, p.partition) for p in partitions}
        before = len(self._buffer)
        self._buffer = [m for m in self._buffer if (m.topic, m.partition) not in gone]
        if before != len(self._buffer):
            log.info("dropped_uncommitted", count=before - len(self._buffer))

