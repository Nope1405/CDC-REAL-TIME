"""Measure CDC latency.

probe   Commit-to-*searchable* latency, measured from outside the pipeline:
        update one product with a unique updated_at, commit, then poll
        Elasticsearch _search (not GET: GET is realtime and ignores refresh)
        until the new value is visible. Run alone or next to load_update.py.

            python benchmark/measure_lag.py probe --count 200 --interval 0.1

report  Per-stage lag from the cdc.* timestamps the consumer writes into each
        document (source -> connector -> kafka -> consumed -> indexed). Only
        the latest version of each document is in the index, so for repeated
        updates of the same id this samples the last one.

            python benchmark/measure_lag.py report --run results/<ts>-load-rate.json
            python benchmark/measure_lag.py report --since-ms 1792483000000

For a full distribution over every event use the consumer histogram
cdc_lag_seconds{stage="e2e"} in Prometheus/Grafana.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone

import psycopg
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from common import es_url, fmt_ms, pg_dsn, summarize, write_result


def probe(args: argparse.Namespace) -> None:
    es = Elasticsearch(es_url(), request_timeout=10)
    lags: list[float] = []
    timeouts = 0
    rng = random.Random()
    with psycopg.connect(pg_dsn(), autocommit=True) as conn:
        lo, hi = conn.execute("SELECT min(id), max(id) FROM products").fetchone()
        for i in range(args.count):
            pid = rng.randint(lo, hi)
            # Millisecond precision: ES `date` stores millis, so the range query must match exactly.
            now = datetime.now(timezone.utc)
            marker = now.replace(microsecond=(now.microsecond // 1000) * 1000)
            conn.execute("UPDATE products SET updated_at = %s WHERE id = %s", (marker, pid))
            committed = time.perf_counter()

            query = {"bool": {"filter": [
                {"term": {"id": pid}},
                {"range": {"updated_at": {"gte": marker.isoformat()}}},
            ]}}
            deadline = committed + args.timeout
            seen = False
            while time.perf_counter() < deadline:
                resp = es.search(index=args.index, query=query, size=0, track_total_hits=True)
                if resp["hits"]["total"]["value"] > 0:
                    lags.append((time.perf_counter() - committed) * 1000)
                    seen = True
                    break
                time.sleep(args.poll)
            if not seen:
                timeouts += 1
            if (i + 1) % 20 == 0:
                print(f"  {i + 1}/{args.count}: {fmt_ms(summarize(lags))} timeouts={timeouts}", flush=True)
            time.sleep(args.interval)

    summary = summarize(lags)
    print(f"commit -> searchable: {fmt_ms(summary)} timeouts={timeouts}")
    path = write_result("probe", {"kind": "probe", "summary_ms": summary, "timeouts": timeouts,
                                  "params": vars(args), "samples_ms": lags})
    print(f"result: {path}")


STAGES = {
    "source->connector": ("source_ts", "connector_ts"),
    "connector->kafka": ("connector_ts", "kafka_ts"),
    "kafka->consumer": ("kafka_ts", "consumed_at"),
    "consumer->indexed": ("consumed_at", "indexed_at"),
    "e2e (commit->indexed)": ("source_ts", "indexed_at"),
}


def report(args: argparse.Namespace) -> None:
    since, until = args.since_ms, args.until_ms
    if args.run:
        run = json.loads(open(args.run, encoding="utf-8").read())
        since, until = run["started_ms"], run["finished_ms"]
    if since is None:
        raise SystemExit("give --run or --since-ms")
    until = until or int(time.time() * 1000)

    es = Elasticsearch(es_url(), request_timeout=60)
    query = {"query": {"range": {"cdc.source_ts": {"gte": since, "lte": until}}}}
    stages: dict[str, list[float]] = {name: [] for name in STAGES}
    indexed: list[int] = []
    for hit in scan(es, index=args.index, query=query, _source=["cdc"], size=2000):
        cdc = hit["_source"].get("cdc", {})
        for name, (a, b) in STAGES.items():
            if cdc.get(a) is not None and cdc.get(b) is not None:
                stages[name].append(max(0, cdc[b] - cdc[a]))
        if cdc.get("indexed_at"):
            indexed.append(cdc["indexed_at"])

    out = {name: summarize(vals) for name, vals in stages.items()}
    for name, summary in out.items():
        print(f"{name:24s} {fmt_ms(summary)}")
    throughput = None
    if len(indexed) > 1 and max(indexed) > min(indexed):
        throughput = len(indexed) / ((max(indexed) - min(indexed)) / 1000)
        print(f"{'throughput (docs/s)':24s} {throughput:.0f}  (distinct docs; see Grafana for events/s)")
    path = write_result("report", {"kind": "report", "index": args.index, "since_ms": since,
                                   "until_ms": until, "stages_ms": out, "throughput_docs_s": throughput,
                                   "run": args.run})
    print(f"result: {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("probe")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--interval", type=float, default=0.2, help="seconds between probes")
    p.add_argument("--poll", type=float, default=0.02, help="ES poll interval (s)")
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--index", default="products")
    p.set_defaults(func=probe)

    r = sub.add_parser("report")
    r.add_argument("--run", help="results/<ts>-load-*.json produced by load_update.py")
    r.add_argument("--since-ms", type=int)
    r.add_argument("--until-ms", type=int)
    r.add_argument("--index", default="products")
    r.set_defaults(func=report)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
