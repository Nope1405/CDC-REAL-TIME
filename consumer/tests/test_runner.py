"""Batch/commit semantics of CdcConsumer + EsSink with in-memory fakes."""

from __future__ import annotations

from confluent_kafka import TopicPartition
from elasticsearch import ConnectionError as EsConnectionError

from app.config import Settings
from app.health import Liveness
from app.runner import CdcConsumer
from app.sink import EsSink, ShutdownRequested

from .conftest import load_raw


class FakeMsg:
    def __init__(self, raw):
        self._r = raw

    def error(self): return None
    def topic(self): return self._r.topic
    def partition(self): return self._r.partition
    def offset(self): return self._r.offset
    def key(self): return self._r.key
    def value(self): return self._r.value
    def headers(self): return list(self._r.headers.items()) or None
    def timestamp(self): return (1, self._r.timestamp_ms)


class FakeConsumer:
    def __init__(self, assignment):
        self._assignment = assignment
        self.commits: list[list[tuple[str, int, int]]] = []
        self.paused = []
        self.resumed = []
        self.polls = 0

    def subscribe(self, topics, **kw): self.subscribed = topics
    def consume(self, num_messages=1, timeout=-1): return []
    def poll(self, timeout=-1):
        self.polls += 1
        return None
    def commit(self, offsets, asynchronous):
        self.commits.append([(o.topic, o.partition, o.offset) for o in offsets])
    def pause(self, p): self.paused.extend(p)
    def resume(self, p): self.resumed.extend(p)
    def assignment(self): return self._assignment
    def close(self): pass


class FakeEs:
    """Returns scripted responses: an Exception instance, or a list of item statuses."""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[list[dict]] = []

    def bulk(self, operations):
        self.calls.append(operations)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        headers = [op for op in operations if set(op) & {"index", "update", "delete"}]
        items = []
        for header, status in zip(headers, step):
            op = next(iter(header))
            body = {"status": status}
            if status >= 400:
                body["error"] = {"type": "mapper_parsing_exception", "reason": "boom"}
            items.append({op: body})
        return {"errors": any(s >= 300 for s in step), "items": items}


class FakeDlq:
    def __init__(self, log):
        self.log = log
        self.sent = []

    def publish(self, entity, msg, reason, error):
        self.sent.append((entity, msg.offset, reason))
        self.log.append(("dlq", msg.offset))

    def flush(self):
        self.log.append(("dlq_flush",))


def make(script, *, assignment=None, **overrides):
    s = Settings(retry_initial_ms=1, retry_max_ms=2, batch_size=100, **overrides)
    consumer = FakeConsumer(assignment or [TopicPartition("cdc.public.products", 3),
                                           TopicPartition("cdc.outbox.order", 1)])
    es = FakeEs(script)
    order: list = []
    dlq = FakeDlq(order)
    runner = CdcConsumer(s, consumer, EsSink(s, client=es), dlq, Liveness(60))
    runner._consumer.commit = _recording_commit(consumer, order)
    return runner, consumer, es, dlq, order


def _recording_commit(consumer, order):
    original = consumer.commit

    def commit(offsets, asynchronous):
        order.append(("commit",))
        original(offsets=offsets, asynchronous=asynchronous)
    return commit


def feed(runner, *names):
    runner._buffer = [load_raw(n) for n in names]


def test_commits_last_offset_plus_one_per_partition_after_success():
    runner, consumer, es, _, _ = make([[201, 201]])
    feed(runner, "product_create", "product_update", "order_created")
    runner.flush()
    # product_create + product_update coalesce into one action
    assert len(es.calls) == 1
    assert sorted(consumer.commits[0]) == [("cdc.outbox.order", 1, 6), ("cdc.public.products", 3, 12)]


def test_rejected_item_goes_to_dlq_before_commit():
    runner, consumer, _, dlq, order = make([[400]])
    feed(runner, "product_update")
    runner.flush()
    assert dlq.sent == [("products", 11, "es_rejected")]
    assert order == [("dlq", 11), ("dlq_flush",), ("commit",)]


def test_parse_error_goes_to_dlq_and_rest_is_indexed():
    runner, consumer, es, dlq, _ = make([[201]])
    feed(runner, "product_bad_price", "order_created")
    runner.flush()
    assert dlq.sent == [("products", 16, "parse_error")]
    assert len(es.calls) == 1
    assert sorted(consumer.commits[0]) == [("cdc.outbox.order", 1, 6), ("cdc.public.products", 3, 17)]


def test_conflict_409_counts_as_success():
    runner, consumer, _, dlq, _ = make([[409]])
    feed(runner, "product_update")
    runner.flush()
    assert dlq.sent == [] and consumer.commits


def test_transient_errors_retry_with_pause_then_resume():
    runner, consumer, es, _, _ = make([EsConnectionError("down"), [503], [201]])
    feed(runner, "product_update")
    runner.flush()
    assert len(es.calls) == 3
    assert consumer.paused and consumer.resumed and consumer.polls > 0
    assert consumer.commits == [[("cdc.public.products", 3, 12)]]


def test_only_failed_items_are_retried():
    runner, _, es, _, _ = make([[201, 429], [201]])
    feed(runner, "product_update", "order_created")
    runner.flush()
    assert len(es.calls[1]) == 2   # one header + one body


def test_shutdown_during_retry_does_not_commit():
    runner, consumer, _, _, _ = make([EsConnectionError("down")] * 5)
    feed(runner, "product_update")
    runner._running = False
    try:
        runner.flush()
        raised = False
    except ShutdownRequested:
        raised = True
    assert raised and consumer.commits == []


def test_does_not_commit_partitions_no_longer_assigned():
    runner, consumer, _, _, _ = make([[201, 201]], assignment=[TopicPartition("cdc.outbox.order", 1)])
    feed(runner, "product_update", "order_created")
    runner.flush()
    assert consumer.commits == [[("cdc.outbox.order", 1, 6)]]


def test_revoke_flushes_and_drops_remaining_buffer():
    runner, consumer, _, _, _ = make([[201]])
    feed(runner, "product_update")
    runner._on_revoke(consumer, [TopicPartition("cdc.public.products", 3)])
    assert consumer.commits == [[("cdc.public.products", 3, 12)]]
    assert runner._buffer == []
