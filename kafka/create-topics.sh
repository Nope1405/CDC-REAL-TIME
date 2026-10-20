#!/usr/bin/env sh
# Creates the CDC data topics and DLQ topics with fixed partition counts before
# any connector starts. Runs in the one-shot kafka-init container.
#
# Partition counts are fixed on purpose: adding partitions later changes the
# key -> partition mapping and breaks per-key ordering (ADR-0004).
# Heartbeat/signal topics are left to the connectors' topic.creation.default.*.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
TOPICS_BIN="${KAFKA_HOME:-/opt/kafka}/bin/kafka-topics.sh"
CDC_PARTITIONS="${CDC_PARTITIONS:-6}"
DLQ_PARTITIONS="${DLQ_PARTITIONS:-3}"

echo "kafka-init: waiting for $BOOTSTRAP"
until "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  sleep 2
done

create() {
  topic="$1"; partitions="$2"; shift 2
  echo "kafka-init: ensuring $topic ($partitions partitions)"
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
}

# Compacted: the latest record per key is kept forever, so the topic can always
# rebuild the index from scratch. Short segments let compaction run during demos.
compacted="--config cleanup.policy=compact
           --config min.cleanable.dirty.ratio=0.1
           --config segment.ms=3600000
           --config delete.retention.ms=86400000
           --config max.message.bytes=2097152"

# shellcheck disable=SC2086
create cdc.public.products "$CDC_PARTITIONS" $compacted
# shellcheck disable=SC2086
create cdc.outbox.order "$CDC_PARTITIONS" $compacted

create cdc.dlq.products "$DLQ_PARTITIONS" \
  --config cleanup.policy=delete --config retention.ms=604800000
create cdc.dlq.orders "$DLQ_PARTITIONS" \
  --config cleanup.policy=delete --config retention.ms=604800000

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe \
  --topic 'cdc\..*' 2>/dev/null | grep -E '^Topic:' || true
echo "kafka-init: done"
