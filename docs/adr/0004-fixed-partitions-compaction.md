# ADR-0004: Fixed partition counts and compacted CDC topics

Status: accepted

## Decision

- `cdc.public.products` and `cdc.outbox.order` have 6 partitions each, created
  by `kafka/create-topics.sh` before the connectors start. Auto-create is disabled.
- The topics use `cleanup.policy=compact`. Debezium emits a tombstone after
  each delete, so compaction can eventually drop deleted keys.
- DLQ topics use `delete` with 7-day retention.

## Why

- Key-to-partition mapping must never change. Adding partitions would split
  one row's history across two partitions and break per-key ordering.
- Six partitions allow up to six consumer replicas.
- Compaction keeps the latest record for every key indefinitely, so the topic
  is always a complete source for rebuilding an index, without unbounded retention.

## Consequences

- Compaction lag (`segment.ms=1h`, `min.cleanable.dirty.ratio=0.1`) means
  older versions stay readable for a while. Replays may re-apply them, which
  external versioning absorbs.
- A consumer that falls behind by more than `delete.retention.ms` (24 h) can
  miss a delete whose tombstone was removed. Treat that as "rebuild required".
