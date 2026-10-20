"""Dead-letter publisher. A record is only committed after its DLQ copy is acknowledged."""

from __future__ import annotations

import socket
import time

import structlog
from confluent_kafka import KafkaException, Producer

from . import metrics
from .config import Settings
from .events import RawMessage

log = structlog.get_logger(__name__)


class DlqPublisher:
    def __init__(self, settings: Settings, producer: Producer | None = None) -> None:
        self._s = settings
        self._producer = producer or Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"cdc-dlq-{socket.gethostname()}",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        })
        self._errors: list[str] = []

    def publish(self, entity: str, msg: RawMessage, reason: str, error: str) -> None:
        headers = {
            "dlq.reason": reason,
            "dlq.error": error[:4000],
            "dlq.source.topic": msg.topic,
            "dlq.source.partition": str(msg.partition),
            "dlq.source.offset": str(msg.offset),
            "dlq.failed_at": str(int(time.time() * 1000)),
        }
        headers.update({f"orig.{k}": v.decode(errors="replace") for k, v in msg.headers.items()})
        self._producer.produce(
            self._s.dlq_topic_for(entity), key=msg.key, value=msg.value,
            headers=list(headers.items()), on_delivery=self._on_delivery)
        metrics.DLQ_TOTAL.labels(entity, reason).inc()
        log.warning("sent_to_dlq", entity=entity, reason=reason, error=error[:300],
                    topic=msg.topic, partition=msg.partition, offset=msg.offset)

    def _on_delivery(self, err, _msg) -> None:  # noqa: ANN001 - librdkafka callback
        if err is not None:
            self._errors.append(str(err))

    def flush(self, timeout_s: float = 30.0) -> None:
        """Block until every DLQ record is acknowledged; raise otherwise (no commit)."""
        remaining = self._producer.flush(timeout_s)
        if remaining or self._errors:
            errors, self._errors = self._errors, []
            raise KafkaException(f"DLQ delivery failed: remaining={remaining} errors={errors[:3]}")
