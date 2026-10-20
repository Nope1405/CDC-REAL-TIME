"""Entrypoint: python -m app.main"""

from __future__ import annotations

import logging
import signal
import socket
import sys

import structlog
from confluent_kafka import Consumer

from .config import Settings
from .dlq import DlqPublisher
from .health import Liveness, serve
from .runner import CdcConsumer
from .sink import EsSink


def configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        cache_logger_on_first_use=True,
    )


def build_consumer(s: Settings) -> Consumer:
    return Consumer({
        "bootstrap.servers": s.kafka_bootstrap_servers,
        "group.id": s.kafka_group_id,
        "client.id": f"cdc-es-sink-{socket.gethostname()}",
        "enable.auto.commit": False,            # commit only after a successful bulk
        "enable.auto.offset.store": False,
        "auto.offset.reset": s.kafka_auto_offset_reset,
        "max.poll.interval.ms": s.kafka_max_poll_interval_ms,
        "session.timeout.ms": s.kafka_session_timeout_ms,
        "partition.assignment.strategy": "range",   # eager: on_revoke sees every partition
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 100,
    })


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger("app")
    log.info("starting", group=settings.kafka_group_id, topics=settings.topic_entities(),
             products_index=settings.es_index_products, orders_index=settings.es_index_orders,
             batch_size=settings.batch_size, delete_mode=settings.delete_mode)

    liveness = Liveness(settings.health_stale_after_s)
    serve(settings.metrics_port, liveness)

    sink = EsSink(settings)
    if not sink.ping():
        log.warning("elasticsearch_not_reachable_yet", url=settings.es_url)

    runner = CdcConsumer(settings, build_consumer(settings), sink, DlqPublisher(settings), liveness)
    signal.signal(signal.SIGTERM, runner.stop)
    signal.signal(signal.SIGINT, runner.stop)
    runner.run()


if __name__ == "__main__":
    main()
