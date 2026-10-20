# Benchmark results

> Template. Fill each `TBD` from `results/*.json`, `benchmark/report.ipynb`
> and Grafana screenshots. Keep the raw files: they are the evidence.

## Environment

| Item | Value |
| --- | --- |
| Host CPU / RAM | TBD |
| Docker Desktop memory / CPUs (WSL2) | TBD |
| Images | postgres 16.4, apache/kafka 3.8.0, debezium/connect 3.0.0.Final, elasticsearch 8.15.3 |
| Consumer | 1 replica, `BATCH_SIZE=500`, `FLUSH_INTERVAL_MS=500`, `DELETE_MODE=soft` |
| Topics | 6 partitions, compacted |
| ES | 1 shard, 0 replicas, `refresh_interval=1s` |
| Dataset | 100k products (~10% with TOASTed description), 10k orders |

## Method

- **Warm-up**: discard the first run after `up`. Run each scenario ≥ 3 times and report the median run.
- **Commit → searchable**: `measure_lag.py probe` (external, includes refresh).
- **Per stage**: `measure_lag.py report --run <load file>` (latest version per document)
  plus Prometheus `cdc_lag_seconds` (every event).
- **Clock**: all components share the Docker host clock, so no NTP skew applies.

## Targets (skills.MD §0)

| Metric | Target | Result | Pass |
| --- | --- | --- | --- |
| e2e p95 at 500 upd/s | < 2 s | TBD | |
| e2e p99 at 500 upd/s | < 5 s | TBD | |
| Consumer throughput | ≥ 5,000 docs/s | TBD | |
| Records lost in chaos tests | 0 | TBD | |
| pgbench TPS drop with CDC | < 5–10 % | TBD | |
| Rebuild 100k products | recorded, no downtime | TBD | |

## Scenarios

| Scenario | PG duration | Drain time (lag → 0) | e2e p50 | p95 | p99 | Throughput |
| --- | --- | --- | --- | --- | --- | --- |
| 10k updates, 1 transaction | TBD | TBD | TBD | TBD | TBD | TBD |
| 10k updates, 10k transactions | TBD | TBD | TBD | TBD | TBD | TBD |
| 500 upd/s × 20 s | TBD | TBD | TBD | TBD | TBD | TBD |

Observations to record: the single large transaction is emitted only at
commit, so its events arrive as a burst. Coalescing shows up in
`cdc_coalesced_total`. Also note which stage dominates the lag.

## Impact on the primary (pgbench, 8 clients, 60 s)

| Setup | TPS | Latency avg | vs baseline |
| --- | --- | --- | --- |
| baseline (no slot) | TBD | TBD | — |
| CDC streaming | TBD | TBD | TBD |
| connector paused (WAL retained) | TBD | TBD | TBD |

## Fault tolerance

| # | Scenario | Result | Evidence |
| --- | --- | --- | --- |
| 01 | kill -9 consumer | TBD | verify output |
| 02 | ES outage 30 s | TBD | pause/resume in Grafana, DLQ unchanged |
| 03 | Connect down, 20k writes | TBD | slot retained before/after |
| 04 | Kafka restart | TBD | |
| 05 | poison messages | TBD | DLQ headers |
| 06 | add/drop column | TBD | |
| 07 | TOAST update | TBD | |
| 08 | rebuild via replay | TBD | duration, alias swap |
| 09 | slot lost + recovery | TBD | |
