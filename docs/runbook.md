# Operations runbook

Commands assume the repo root, Git Bash/WSL for `curl`, and the default ports.
`$PSQL` stands for `docker compose exec postgres psql -U cdc_app -d cdc`.

## Health at a glance

```bash
docker compose ps
curl -s localhost:8083/connectors?expand=status | jq '.[] | {name: .status.name, state: .status.connector.state, tasks: [.status.tasks[].state]}'
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group cdc-es-sink
$PSQL -c "SELECT slot_name, active, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots"
```

Grafana: <http://localhost:3000>, dashboard **CDC pipeline**.

## Connector failed {#connector-failed}

Symptoms: `ConnectTaskFailed`, `CdcReplicationSlotInactive`, `debezium_metrics_*` flat.

1. Read the trace: `curl -s localhost:8083/connectors/debezium-products/status | jq -r '.tasks[0].trace' | head -40`.
2. Fix the cause (DB credentials, a missing publication or table, a broken SMT config).
3. Restart only the failed parts:
   `curl -X POST 'localhost:8083/connectors/debezium-products/restart?includeTasks=true&onlyFailed=true'`.
4. After editing `connect/*.json`, re-apply with `.\dev.ps1 register` (PUT is idempotent).

## Replication slot retaining WAL {#slot-lag}

Symptoms: `CdcReplicationSlotRetainingWal` / `CdcReplicationSlotNearInvalidation`.

1. Is the connector running? Check its status (see above). A paused or failed
   connector is the usual cause.
2. Is the connector running but slow? Look at `MilliSecondsBehindSource` and
   `QueueRemainingCapacity`, and the Kafka produce errors in the Connect log.
3. If the slot is close to `max_slot_wal_keep_size` and the connector cannot be
   fixed quickly, the database is protected by design: the slot is invalidated,
   not the disk filled. Plan for [slot lost](#slot-lost).
4. **Never** drop a slot that belongs to a healthy connector. Debezium would
   continue from a position whose WAL no longer exists, and the gap would be
   silent data loss.

## Slot lost {#slot-lost}

`wal_status = 'lost'`: the WAL the connector still needs is gone. Streaming
cannot continue, so the connector must re-snapshot.

1. Delete the connector: `curl -X DELETE localhost:8083/connectors/debezium-products`.
2. Drop the slot: `$PSQL -c "SELECT pg_drop_replication_slot('cdc_products_slot')"`.
3. Register it again **under a new connector name**, so Kafka Connect has no
   stored offset and runs `snapshot.mode=initial` from scratch. For example,
   change `"name"` and the filename to `debezium-products-r2`, then run
   `.\dev.ps1 register`. Alternative: keep the name, delete its offsets with
   the Connect offsets REST API while the connector is stopped
   (`PUT /connectors/<name>/stop`, `DELETE /connectors/<name>/offsets`), then resume.
4. The snapshot re-emits every row with a newer LSN, so ES converges. Run
   `.\dev.ps1 verify`. Rows deleted during the gap are still live in ES.
   `verify` lists them as `extra`, and a rebuild into a new index removes them.
5. Drill: `benchmark/chaos/09-slot-lost.sh --yes`.

## Elasticsearch down or rejecting {#es-down}

The consumer pauses and retries on its own (`CdcConsumerPaused`). No action is
needed for an outage. When ES is back, lag drains. Confirm with `.\dev.ps1 verify`.
If instead the DLQ is growing with `es_rejected`, it is a mapping problem: see [DLQ replay](#dlq-replay).

## DLQ replay {#dlq-replay}

1. Inspect: `python benchmark/replay_dlq.py --entity products --list`.
   Each record carries `dlq.reason`, `dlq.error`, and the source topic, partition and offset.
2. Fix the root cause. `parse_error` usually means a connector config
   regression (for example `decimal.handling.mode`) or a consumer bug.
   `es_rejected` usually means a mapping conflict.
3. Replay: `python benchmark/replay_dlq.py --entity products [--reason es_rejected]`.
   Records go back to their source topic with their original version, so
   stale ones cannot overwrite newer data.

## Reset consumer offsets

Stop all consumers in the group first (an active group refuses the reset):

```bash
docker compose stop consumer
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --group cdc-es-sink --topic cdc.public.products --reset-offsets --to-earliest --execute
docker compose start consumer
```

Re-reading is safe because writes are versioned and idempotent.

## Rebuild index {#rebuild-index}

Use this after a mapping change that needs a new index (an analyzer or field
type change), or to remove drift.

```powershell
.\dev.ps1 rebuild 2          # = benchmark/chaos/08-rebuild-index.sh 2
```

Steps the script performs:

1. `reindex.sh prepare products 2` creates `products_v2` from the template with refresh disabled.
2. It starts a consumer with group `cdc-es-rebuild-products-v2`,
   `ENTITIES=products`, `ES_INDEX_PRODUCTS=products_v2`, reading from the earliest offset.
3. It waits for that group's lag to reach 0.
4. `reindex.sh switch products 2` restores the refresh interval and swaps the alias in a single `_aliases` call.
5. It stops the rebuild consumer. Delete `products_v1` once you are satisfied.

For **orders**, the topic only holds what was emitted since the connector
started (no snapshot). To repopulate from the database, run
`SELECT backfill_orders_outbox();` and rebuild from the replay.

## Incremental snapshot (re-sync one table without stopping streaming)

```sql
INSERT INTO debezium_signal (id, type, data)
VALUES (gen_random_uuid()::text, 'execute-snapshot',
        '{"data-collections": ["public.products"], "type": "incremental"}');
```

Progress is visible in the Connect log. Snapshot chunks carry current LSNs, so
they are safe to interleave with live changes.

## Schema change on a captured table

- **Add column**: nothing breaks. The field lands in `_source`. To make it
  searchable, add it to `elasticsearch/<entity>-mapping.json` and `PUT
  <alias>/_mapping`, or rebuild.
- **Drop or rename column**: later events stop carrying it, and older
  documents keep the old field until rebuilt. A rename is a drop plus an add.
- **Type change**: needs a new index version (rebuild).
- Drill: `benchmark/chaos/06-schema-evolution.sh`.

## Rollback

- Index: run `reindex.sh switch <entity> <old_version>` to point the alias
  back. The old index is kept until deleted.
- Connector config: re-PUT the previous JSON from git.
- Consumer: redeploy the previous image. Offsets and versions make re-processing safe.
