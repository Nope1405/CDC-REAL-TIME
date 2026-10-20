"""Generate dashboards/cdc-pipeline.json. Edit here, then: python monitoring/grafana/build_dashboard.py"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
# Fixed series colours: reference palette slots 1-3 (blue, orange, aqua), validated
# as a set for colour-vision deficiency. Status colours are reserved for thresholds.
C = ["#2a78d6", "#eb6834", "#1baf7a"]
PCT = {"p50": C[0], "p95": C[1], "p99": C[2]}
_ids = iter(range(1, 1000))


def target(i, expr, legend=""):
    return {"refId": chr(65 + i), "datasource": DS, "expr": expr, "legendFormat": legend}


def timeseries(title, targets, unit, x, y, w=12, h=8, colors=None, desc="", threshold=None, stack=False):
    defaults = {
        "unit": unit, "min": 0, "color": {"mode": "palette-classic"},
        "custom": {
            "lineWidth": 2, "fillOpacity": 60 if stack else 0, "showPoints": "never", "spanNulls": True,
            "stacking": {"mode": "normal" if stack else "none"},
            "thresholdsStyle": {"mode": "dashed" if threshold is not None else "off"},
        },
    }
    if threshold is not None:
        defaults["thresholds"] = {"mode": "absolute", "steps": [
            {"color": "transparent", "value": None}, {"color": "red", "value": threshold}]}
    overrides = [
        {"matcher": {"id": "byName", "options": name},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
        for name, c in (colors or {}).items()
    ]
    return {
        "id": next(_ids), "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults, "overrides": overrides},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [target(i, e, legend) for i, (e, legend) in enumerate(targets)],
    }


def stat(title, expr, unit, x, y, steps, desc="", w=4, h=4):
    return {
        "id": next(_ids), "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {
            "unit": unit, "color": {"mode": "thresholds"},
            "thresholds": {"mode": "absolute",
                           "steps": [{"color": c, "value": v} for c, v in steps]}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background",
                    "graphMode": "none", "textMode": "value"},
        "targets": [target(0, expr)],
    }


def row(title, y):
    return {"id": next(_ids), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def lag_q(p, stage):
    return (f'histogram_quantile({p}, sum by (le) (rate(cdc_lag_seconds_bucket'
            f'{{stage="{stage}",entity=~"$entity"}}[$__rate_interval])))')


def bulk_q(p):
    return f"histogram_quantile({p}, sum by (le) (rate(cdc_bulk_duration_seconds_bucket[$__rate_interval])))"


OK, WARN, CRIT = "green", "orange", "red"

panels = [
    row("Overview", 0),
    stat("p95 commit → indexed", lag_q(0.95, "e2e"), "s", 0, 1, [(OK, None), (WARN, 1), (CRIT, 2)],
         "Target < 2 s. Searchable adds up to refresh_interval (1 s)."),
    stat("Indexed docs/s", 'sum(rate(cdc_docs_indexed_total{entity=~"$entity"}[$__rate_interval]))',
         "ops", 4, 1, [("blue", None)]),
    stat("Consumer lag (records)", 'sum(kafka_consumergroup_lag{consumergroup="$group"})', "short", 8, 1,
         [(OK, None), (WARN, 1000), (CRIT, 10000)]),
    stat("Debezium behind source", 'max(debezium_metrics_MilliSecondsBehindSource{context="streaming"})',
         "ms", 12, 1, [(OK, None), (WARN, 5000), (CRIT, 30000)], "Requires the JMX exporter."),
    stat("Max WAL retained by a slot", "max(cdc_replication_slot_retained_bytes)", "bytes", 16, 1,
         [(OK, None), (WARN, 5e8), (CRIT, 1e9)]),
    stat("DLQ records (last 1h)", 'sum(increase(cdc_dlq_total{entity=~"$entity"}[1h]))', "short", 20, 1,
         [(OK, None), (CRIT, 1)]),

    row("Latency", 5),
    timeseries("Commit → indexed lag", [(lag_q(p / 100, "e2e"), f"p{p}") for p in (50, 95, 99)], "s", 0, 6,
               colors=PCT, threshold=2,
               desc="cdc_lag_seconds{stage=e2e}: PG commit (source.ts_ms) → bulk acknowledged. Dashed line = 2 s target."),
    timeseries("p95 lag per pipeline stage",
               [(lag_q(0.95, s), s) for s in ("source", "kafka", "consume", "index")], "s", 12, 6, stack=True,
               desc="source: PG→Debezium · kafka: Debezium→Kafka · consume: Kafka→consumer · "
                    "index: consumer→ES ack. Stacked, so the top edge approximates e2e."),
    timeseries("Elasticsearch bulk duration", [(bulk_q(p / 100), f"p{p}") for p in (50, 95, 99)], "s", 0, 14,
               colors=PCT, desc="Includes retries and backoff, so an ES outage shows up here."),
    timeseries("Debezium MilliSecondsBehindSource",
               [('debezium_metrics_MilliSecondsBehindSource{context="streaming"}', "{{name}}")], "ms", 12, 14),

    row("Throughput & backlog", 22),
    timeseries("Events consumed vs docs indexed",
               [('sum by (entity) (rate(cdc_events_consumed_total{entity=~"$entity"}[$__rate_interval]))',
                 "consumed {{entity}}"),
                ('sum by (entity) (rate(cdc_docs_indexed_total{entity=~"$entity"}[$__rate_interval]))',
                 "indexed {{entity}}")], "ops", 0, 23,
               desc="indexed < consumed when a batch coalesces several updates of the same document."),
    timeseries("Kafka consumer lag by topic",
               [('sum by (topic) (kafka_consumergroup_lag{consumergroup="$group"})', "{{topic}}")], "short", 12, 23),
    timeseries("WAL retained per replication slot", [("cdc_replication_slot_retained_bytes", "{{slot_name}}")],
               "bytes", 0, 31, threshold=1e9,
               desc="Grows while a connector is down; capped by max_slot_wal_keep_size (2 GB)."),
    timeseries("WAL write rate", [("rate(cdc_wal_lsn_bytes[$__rate_interval])", "WAL bytes/s")], "Bps", 12, 31,
               colors={"WAL bytes/s": C[0]}),

    row("Errors", 39),
    timeseries("Bulk errors by type", [("sum by (type) (rate(cdc_bulk_errors_total[$__rate_interval]))", "{{type}}")],
               "ops", 0, 40, w=8),
    timeseries("Dead-lettered records",
               [('sum by (entity, reason) (increase(cdc_dlq_total{entity=~"$entity"}[$__rate_interval]))',
                 "{{entity}} {{reason}}")], "short", 8, 40, w=8),
    timeseries("Partitions paused (ES back-pressure)", [("max(cdc_partitions_paused)", "paused")], "short",
               16, 40, w=8, colors={"paused": C[1]}),
]

dashboard = {
    "uid": "cdc-pipeline", "title": "CDC pipeline", "tags": ["cdc"], "timezone": "utc",
    "schemaVersion": 39, "version": 1, "refresh": "5s", "graphTooltip": 1,
    "time": {"from": "now-30m", "to": "now"},
    "templating": {"list": [
        {"name": "entity", "label": "Entity", "type": "custom", "query": "products,orders",
         "includeAll": True, "allValue": ".*", "multi": True,
         "current": {"text": "All", "value": "$__all"}, "options": []},
        {"name": "group", "type": "constant", "query": "cdc-es-sink", "hide": 2},
    ]},
    "panels": panels,
}

out = Path(__file__).parent / "dashboards" / "cdc-pipeline.json"
out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"wrote {out} ({len(panels)} panels)")
