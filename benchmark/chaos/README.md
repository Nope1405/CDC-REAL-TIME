# Chaos scenarios

Fault-injection scripts for the claims in `skills.MD` section 8. Each script
injects one failure, keeps (or generates) load, and ends with
`verify_consistency.py`, which compares PostgreSQL and Elasticsearch row by
row (count + checksum + mismatched ids). Exit code 0 = PASS.

## Prerequisites

- Core stack running and seeded (`.\dev.ps1 up`, `.\dev.ps1 seed`).
- Host venv with `benchmark/requirements.txt` (`.\dev.ps1 venv`).
- Run from Git Bash or WSL (scripts are bash). Example:
  `bash benchmark/chaos/01-kill-consumer.sh`

## Scenarios

| # | Script | Failure | Expected result |
| --- | --- | --- | --- |
| 01 | `01-kill-consumer.sh` | `SIGKILL` the consumer mid-load | Re-consumes uncommitted batch; no loss, duplicates absorbed by external version |
| 02 | `02-es-outage.sh` | Elasticsearch stopped ~30 s | Backoff + `pause()`, stays in group, catches up; DLQ unchanged |
| 03 | `03-connect-outage-wal.sh` | Kafka Connect stopped, 20k writes | `pg_replication_slots` retained WAL grows, then drains; no loss |
| 04 | `04-kafka-restart.sh` | Broker restart under load | Connect + consumer reconnect; no loss |
| 05 | `05-poison-message.sh` | Invalid JSON / missing LSN / base64 decimal | Records in `cdc.dlq.products` with `dlq.*` headers; pipeline continues |
| 06 | `06-schema-evolution.sh` | `ADD COLUMN`, then `DROP COLUMN` | Field kept in `_source`; searchable after mapping update; no downtime |
| 07 | `07-toast-update.sh [--default]` | Update rows with TOASTed `description` | Placeholder never reaches ES (partial-update path) |
| 08 | `08-rebuild-index.sh [V]` | Throw the index away | Replay compacted topic into `products_vV`, atomic alias swap |
| 09 | `09-slot-lost.sh --yes` | Slot exceeds `max_slot_wal_keep_size` | `wal_status=lost`; recover by drop slot + re-snapshot (**destructive**) |

## Recording results

Paste the PASS/FAIL line, the slot/lag snapshots and the verify output into
`docs/results.md` (section "Fault tolerance"). Grafana's *CDC pipeline*
dashboard shows the pause, lag spike and recovery for 02-04.
