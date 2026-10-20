# CDC Search Sync

Real-time sync from PostgreSQL to Elasticsearch: **PostgreSQL → Debezium → Kafka → Python consumer → Elasticsearch**,
with measured lag, at-least-once delivery with idempotent writes, and fault-injection tests.

- Design: [docs/architecture.md](docs/architecture.md) · decisions: [docs/adr/](docs/adr/README.md)
- Operations: [docs/runbook.md](docs/runbook.md) · results: [docs/results.md](docs/results.md)
- Plan and checklist (Vietnamese): [skills.MD](skills.MD)

## Quick start (Windows PowerShell)

Prerequisites: Docker Desktop (WSL2, ≥ 10 GB memory), Python 3.12+, Git for
Windows (bash, for the chaos scripts). Elasticsearch needs
`wsl -d docker-desktop sysctl -w vm.max_map_count=262144`.

```powershell
.\dev.ps1 venv          # host venv: tests + benchmark scripts
.\dev.ps1 test          # consumer unit tests, no Docker needed
.\dev.ps1 up            # creates .env, starts the stack, topics, ES indices, connectors
.\dev.ps1 seed          # 100k products + 10k orders
.\dev.ps1 verify        # PostgreSQL == Elasticsearch ?
```

On Linux/macOS/WSL use `make <target>` instead (same target names).

| UI | URL |
| --- | --- |
| Kafka UI | <http://localhost:8080> |
| Kibana | <http://localhost:5601> |
| Kafka Connect REST | <http://localhost:8083/connectors?expand=status> |
| Consumer metrics | <http://localhost:8000/metrics> (first replica) |
| Grafana (`.\dev.ps1 monitoring`) | <http://localhost:3000> |

## Try it

```powershell
docker compose exec postgres psql -U cdc_app -d cdc -c "UPDATE products SET name = 'Điện thoại thử nghiệm' WHERE id = 1"
curl.exe -s "localhost:9200/products/_search?q=name:dien%20thoai%20thu%20nghiem&filter_path=hits.hits._source.name"
```

## Benchmarks and chaos tests

```powershell
.\dev.ps1 bench-rate    # 500 updates/s for 20 s
.\dev.ps1 report        # per-stage lag of that run
.\dev.ps1 probe         # commit -> searchable, measured from outside
.\dev.ps1 pgbench       # cost of CDC on the primary
.\dev.ps1 chaos 01      # kill -9 the consumer mid-load (see benchmark/chaos/README.md)
```

## Layout

```text
docker-compose.yml   core stack + profiles: monitoring, bench
postgres/            config, schema, Debezium role, publications, outbox function
kafka/               topic creation (fixed partitions, compaction, DLQ)
connect/             connector configs, register script, JMX exporter rules
elasticsearch/       component/index templates, mappings, bootstrap, reindex
consumer/            Python consumer (app/), Dockerfile, tests + Debezium fixtures
benchmark/           seed, load, lag measurement, consistency check, DLQ replay, pgbench, chaos/
monitoring/          Prometheus config + alerts, postgres-exporter queries, Grafana
docs/                architecture, runbook, results, ADRs
```
