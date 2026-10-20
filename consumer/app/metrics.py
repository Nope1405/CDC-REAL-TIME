"""Prometheus metrics exported on :8000/metrics."""

from prometheus_client import Counter, Gauge, Histogram

LAG_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30, 60, 120, 300)

EVENTS_CONSUMED = Counter(
    "cdc_events_consumed_total", "Kafka records consumed", ["entity", "op"])
DOCS_INDEXED = Counter(
    "cdc_docs_indexed_total", "Bulk items acknowledged by Elasticsearch", ["entity", "result"])
DLQ_TOTAL = Counter(
    "cdc_dlq_total", "Records sent to the dead-letter topic", ["entity", "reason"])
BULK_ERRORS = Counter(
    "cdc_bulk_errors_total", "Bulk failures by class", ["type"])
BULK_DURATION = Histogram(
    "cdc_bulk_duration_seconds", "Elasticsearch bulk request latency (including retries)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
BATCH_SIZE = Histogram(
    "cdc_batch_size", "Records per flushed batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000, 2000, 5000))
COALESCED = Counter(
    "cdc_coalesced_total", "Actions dropped because a newer action for the same doc was in the batch",
    ["entity"])

# End-to-end and per-stage lag, in seconds:
#   source:    PG commit (source.ts_ms)      -> Debezium (ts_ms)
#   kafka:     Debezium                       -> Kafka record timestamp
#   consume:   Kafka record                   -> consumer received
#   index:     consumer received              -> bulk acknowledged
#   e2e:       PG commit                      -> bulk acknowledged (searchable <= e2e + refresh_interval)
LAG = Histogram(
    "cdc_lag_seconds", "CDC lag per pipeline stage", ["entity", "stage"], buckets=LAG_BUCKETS)

COMMITS = Counter("cdc_offset_commits_total", "Offset commits", ["result"])
PAUSED = Gauge("cdc_partitions_paused", "1 while partitions are paused waiting for Elasticsearch")
ASSIGNED = Gauge("cdc_partitions_assigned", "Partitions currently assigned to this consumer")
LAST_SOURCE_TS = Gauge(
    "cdc_last_source_timestamp_seconds", "source.ts_ms of the last indexed event", ["entity"])
