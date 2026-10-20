"""Compare PostgreSQL (source of truth) with Elasticsearch (derived data).

    python benchmark/verify_consistency.py                     # products + orders
    python benchmark/verify_consistency.py --entity products --wait 120

Compares, per live row: id plus a few business columns, and reports
  missing   rows in PG but not (live) in ES
  extra     live docs in ES that PG no longer has (missed delete)
  mismatch  same id, different values (stale update)
and an order-independent SHA-256 checksum of each side.
--wait N retries for up to N seconds (the pipeline may still be catching up).
Exit code 0 = consistent, 1 = differences.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import sys
import time
from datetime import datetime
from decimal import Decimal

import psycopg
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from common import es_url, pg_dsn

CENT = Decimal("0.01")


def money(v) -> str:
    return str(Decimal(str(v)).quantize(CENT)) if v is not None else ""


def ts_ms(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, str):
        v = datetime.fromisoformat(v.replace("Z", "+00:00"))
    # Integer math (no float rounding); ES `date` keeps millisecond precision.
    return calendar.timegm(v.utctimetuple()) * 1000 + v.microsecond // 1000


ENTITIES = {
    "products": {
        "sql": "SELECT id, price, stock, is_active, updated_at FROM products",
        "fields": ["id", "price", "stock", "is_active", "updated_at"],
        "pg": lambda r: (r[0], money(r[1]), r[2], r[3], ts_ms(r[4])),
        "es": lambda d: (d["id"], money(d.get("price")), d.get("stock"), d.get("is_active"),
                         ts_ms(d.get("updated_at"))),
    },
    "orders": {
        "sql": ("SELECT o.id, o.status, o.total_amount, count(i.id) FROM orders o "
                "LEFT JOIN order_items i ON i.order_id = o.id GROUP BY o.id"),
        "fields": ["id", "status", "total_amount", "item_count"],
        "pg": lambda r: (r[0], r[1], money(r[2]), r[3]),
        "es": lambda d: (d["id"], d.get("status"), money(d.get("total_amount")), d.get("item_count")),
    },
}


def load_pg(conn: psycopg.Connection, entity: str) -> dict[int, tuple]:
    spec = ENTITIES[entity]
    return {row[0]: spec["pg"](row) for row in conn.execute(spec["sql"])}


def load_es(es: Elasticsearch, entity: str) -> dict[int, tuple]:
    spec = ENTITIES[entity]
    query = {"query": {"bool": {"must_not": [{"term": {"is_deleted": True}}]}}}
    out = {}
    for hit in scan(es, index=entity, query=query, _source=spec["fields"], size=5000):
        row = spec["es"](hit["_source"])
        out[row[0]] = row
    return out


def checksum(rows: dict[int, tuple]) -> str:
    h = hashlib.sha256()
    for key in sorted(rows):
        h.update(repr(rows[key]).encode())
    return h.hexdigest()[:16]


def compare(entity: str, pg: dict[int, tuple], es: dict[int, tuple]) -> dict:
    missing = sorted(pg.keys() - es.keys())
    extra = sorted(es.keys() - pg.keys())
    mismatch = sorted(k for k in pg.keys() & es.keys() if pg[k] != es[k])
    return {"entity": entity, "pg_count": len(pg), "es_count": len(es),
            "pg_checksum": checksum(pg), "es_checksum": checksum(es),
            "missing": missing, "extra": extra, "mismatch": mismatch,
            "ok": not (missing or extra or mismatch), "samples": {k: (pg[k], es[k]) for k in mismatch[:5]}}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entity", choices=["products", "orders", "all"], default="all")
    ap.add_argument("--wait", type=float, default=0, help="keep retrying for N seconds")
    args = ap.parse_args()

    entities = list(ENTITIES) if args.entity == "all" else [args.entity]
    es = Elasticsearch(es_url(), request_timeout=60)
    deadline = time.monotonic() + args.wait
    while True:
        es.indices.refresh(index=",".join(entities))
        with psycopg.connect(pg_dsn()) as conn:
            results = [compare(e, load_pg(conn, e), load_es(es, e)) for e in entities]
        if all(r["ok"] for r in results) or time.monotonic() >= deadline:
            break
        print("not consistent yet, retrying ...", flush=True)
        time.sleep(5)

    for r in results:
        status = "OK " if r["ok"] else "FAIL"
        print(f"[{status}] {r['entity']:8s} pg={r['pg_count']} es={r['es_count']} "
              f"checksum pg={r['pg_checksum']} es={r['es_checksum']} "
              f"missing={len(r['missing'])} extra={len(r['extra'])} mismatch={len(r['mismatch'])}")
        for label in ("missing", "extra", "mismatch"):
            if r[label]:
                print(f"       {label} ids (first 20): {r[label][:20]}")
        for k, (p, e) in r["samples"].items():
            print(f"       id={k}\n         pg={p}\n         es={e}")
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()
