# Technology and prerequisite check

## Required runtime stack

- Docker Desktop with the WSL2 backend and Docker Compose v2.
- PostgreSQL with logical replication support (the pinned Docker image will provide this).
- Apache Kafka in KRaft mode, one broker for development.
- Debezium PostgreSQL connector 3.x, pinned to an exact image/plugin version.
- Elasticsearch and Kibana, pinned to matching versions.
- Kafka Connect and a Kafka UI/Redpanda Console, pinned to exact versions.
- Python 3.12 in the consumer image; Python 3.12/3.13 on the host with a virtual environment for local scripts and tests.
- `confluent-kafka`, `elasticsearch`, `pydantic` v2, `pydantic-settings`,
  `prometheus-client` and a retry library, all pinned in `requirements.txt`.

## Required development tools

- Git and a POSIX-compatible shell for `.sh` scripts. On Windows, run scripts
  inside WSL or invoke them through their containers; keep shell files LF.
- `curl` for Connect/Elasticsearch health and registration calls.
- `psql` and `pgbench` for schema checks and the CDC load scenarios.
- Python test tooling (`pytest`, optionally `testcontainers`) and a JSON-capable
  editor/formatter.
- Docker Desktop memory allocation sufficient for PostgreSQL, Kafka, Connect,
  Elasticsearch, Kibana and monitoring. Start with at least 10 GB for this stack.
- WSL2 `vm.max_map_count=262144` for Elasticsearch.

## Decisions (previously open)

| # | Question | Decision |
| --- | --- | --- |
| 1 | Image versions | postgres 16.4-bookworm, apache/kafka 3.8.0, quay.io/debezium/connect 3.0.0.Final, elasticsearch/kibana 8.15.3, kafka-ui v0.7.2, prometheus v2.54.1, grafana 11.2.2 (`docker-compose.yml`) |
| 2 | Kafka UI vs Redpanda Console | Kafka UI (provectuslabs) |
| 3 | Integration tests | Compose stack + `benchmark/chaos/*.sh` + `verify_consistency.py`; unit tests use in-memory fakes |
| 4 | ES mapping policy | `dynamic: false` ([ADR-0006](adr/0006-json-mapping-dynamic-false.md)) |
| 5 | Hard vs soft delete | Soft by default, `CONSUMER_DELETE_MODE=hard` available ([ADR-0003](adr/0003-soft-delete.md)) |
| 6 | Partitions / topics | 6 partitions, compacted `cdc.public.products`, `cdc.outbox.order`; DLQ `cdc.dlq.*` 7 d ([ADR-0004](adr/0004-fixed-partitions-compaction.md)) |
| 7 | Order fan-out on product rename | None, names are snapshotted ([ADR-0005](adr/0005-no-fanout-product-name.md)) |
| 8 | Searchable latency method | External probe polling `_search` (`measure_lag.py probe`) + per-stage `cdc.*` timestamps; 100k products / 10k orders |
| 9 | Avro + Schema Registry | Deferred; JSON without schemas in phase one |
| 10 | Secrets | `.env` locally; Connect reads DB credentials via `EnvVarConfigProvider` (`${env:...}`), never from connector JSON |

## Windows-specific readiness

- The current workspace path contains spaces and `&`; quote it in commands and
  avoid interpolating it into shell scripts. Renaming the folder is optional but
  reduces friction.
- Confirm Docker Desktop exposes the WSL2 engine and that the allocated memory
  is enough before starting Elasticsearch.
- Do not commit `.env`; use `.env.example` as the contract for local settings.

## Suggested implementation order

1. Pin versions and complete `docker-compose.yml` plus healthchecks.
2. Add PostgreSQL schema/publication and reproducible seed data.
3. Register the products connector and validate raw Kafka events.
4. Add Elasticsearch templates/mappings and alias bootstrap.
5. Implement the consumer and its unit fixtures.
6. Add DLQ/replay, metrics, consistency checks and integration tests.
7. Add orders outbox flow, benchmark scenarios, chaos tests and runbook evidence.
