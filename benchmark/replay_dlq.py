"""Inspect or replay dead-lettered records.

    python benchmark/replay_dlq.py --entity products --list
    python benchmark/replay_dlq.py --entity products               # re-publish to source topic
    python benchmark/replay_dlq.py --entity products --reason es_rejected

Replay copies key/value (and the original headers) back to the topic named in
the dlq.source.topic header, so the normal consumer processes them again after
the bug/mapping is fixed. Progress is tracked by consumer group
cdc-dlq-replayer-<entity>, committed only after the produce is acknowledged,
so re-running continues where it stopped. Replayed records keep their
original version/LSN, so a stale record can never overwrite newer data.
"""

from __future__ import annotations

import argparse
import os

from confluent_kafka import Consumer, Producer

from common import load_env


def headers_dict(msg) -> dict[str, str]:
    return {k: (v or b"").decode(errors="replace") for k, v in (msg.headers() or [])}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entity", choices=["products", "orders"], required=True)
    ap.add_argument("--reason", help="only records with this dlq.reason (parse_error, es_rejected)")
    ap.add_argument("--list", action="store_true", help="print records, do not replay or commit")
    ap.add_argument("--idle-timeout", type=float, default=5.0)
    args = ap.parse_args()

    load_env()
    bootstrap = f"localhost:{os.environ.get('KAFKA_EXTERNAL_PORT', '9092')}"
    topic = f"cdc.dlq.{args.entity}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"cdc-dlq-replayer-{args.entity}" + ("-list" if args.list else ""),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    consumer.subscribe([topic])

    seen = replayed = 0
    try:
        while True:
            msg = consumer.poll(args.idle_timeout)
            if msg is None:
                break
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            seen += 1
            h = headers_dict(msg)
            if args.reason and h.get("dlq.reason") != args.reason:
                continue
            if args.list:
                print(f"[{msg.offset()}] {h.get('dlq.reason')} from {h.get('dlq.source.topic')}"
                      f"[{h.get('dlq.source.partition')}]@{h.get('dlq.source.offset')}: {h.get('dlq.error')}")
                print(f"      key={msg.key()!r} value={(msg.value() or b'')[:200]!r}")
                continue
            original = [(k[5:], v.encode()) for k, v in h.items() if k.startswith("orig.")]
            producer.produce(h["dlq.source.topic"], key=msg.key(), value=msg.value(), headers=original)
            if producer.flush(30) != 0:
                raise SystemExit("produce not acknowledged; stopping without commit")
            consumer.commit(message=msg, asynchronous=False)
            replayed += 1
    finally:
        consumer.close()
    action = "listed" if args.list else "replayed"
    print(f"{topic}: read {seen}, {action} {seen if args.list else replayed}")


if __name__ == "__main__":
    main()
