"""Generate controlled product/order updates for throughput and lag tests.

Scenarios (docs/results.md):
    python benchmark/load_update.py single-tx --count 10000      # one big transaction
    python benchmark/load_update.py per-tx    --count 10000      # 10k autocommit transactions
    python benchmark/load_update.py rate      --rate 500 --duration 20
    python benchmark/load_update.py rate      --rate 50 --duration 60 --entity orders

Writes results/<ts>-load-<mode>.json with the time window, which
measure_lag.py report --run <file> uses to select documents.
"""

from __future__ import annotations

import argparse
import random
import time

import psycopg

from common import pg_dsn, write_result

PRODUCT_UPDATE = (
    "UPDATE products SET price = price + 1000, stock = (stock + 1) %% 1000 WHERE id = %s"
)
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered"]


def id_range(conn: psycopg.Connection, table: str) -> tuple[int, int]:
    lo, hi = conn.execute(f"SELECT min(id), max(id) FROM {table}").fetchone()  # noqa: S608 - fixed names
    if lo is None:
        raise SystemExit(f"{table} is empty; run seed.py first")
    return lo, hi


def update_one(cur: psycopg.Cursor, entity: str, rid: int, rng: random.Random) -> None:
    if entity == "products":
        cur.execute(PRODUCT_UPDATE, (rid,))
    else:
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", (rng.choice(ORDER_STATUSES), rid))
        cur.execute("SELECT emit_order_event(%s, 'OrderUpdated')", (rid,))


def run_single_tx(conn, entity, ids, rng) -> int:
    with conn.transaction(), conn.cursor() as cur:
        for rid in ids:
            update_one(cur, entity, rid, rng)
    return len(ids)


def run_per_tx(conn, entity, ids, rng) -> int:
    conn.autocommit = True
    with conn.cursor() as cur:
        for rid in ids:
            if entity == "orders":
                with conn.transaction():
                    update_one(cur, entity, rid, rng)
            else:
                update_one(cur, entity, rid, rng)
    return len(ids)


def run_rate(conn, entity, lo, hi, rng, rate: float, duration: float) -> int:
    conn.autocommit = True
    interval = 1.0 / rate
    done = 0
    start = time.perf_counter()
    next_at = start
    with conn.cursor() as cur:
        while (now := time.perf_counter()) - start < duration:
            if now < next_at:
                time.sleep(next_at - now)
            rid = rng.randint(lo, hi)
            if entity == "orders":
                with conn.transaction():
                    update_one(cur, entity, rid, rng)
            else:
                update_one(cur, entity, rid, rng)
            done += 1
            next_at += interval
            if done % max(1, int(rate)) == 0:
                elapsed = time.perf_counter() - start
                print(f"  {done} updates, {done / elapsed:.0f}/s", flush=True)
    return done


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["single-tx", "per-tx", "rate"])
    ap.add_argument("--entity", choices=["products", "orders"], default="products")
    ap.add_argument("--count", type=int, default=10_000)
    ap.add_argument("--rate", type=float, default=500)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    with psycopg.connect(pg_dsn()) as conn:
        lo, hi = id_range(conn, args.entity)
        ids = rng.sample(range(lo, hi + 1), min(args.count, hi - lo + 1))
        conn.commit()

        started_ms = int(time.time() * 1000)
        t0 = time.perf_counter()
        if args.mode == "single-tx":
            n = run_single_tx(conn, args.entity, ids, rng)
        elif args.mode == "per-tx":
            n = run_per_tx(conn, args.entity, ids, rng)
        else:
            n = run_rate(conn, args.entity, lo, hi, rng, args.rate, args.duration)
        elapsed = time.perf_counter() - t0
        finished_ms = int(time.time() * 1000)

    print(f"{args.mode}: {n} {args.entity} updates committed in {elapsed:.2f}s ({n / elapsed:.0f}/s)")
    path = write_result(f"load-{args.mode}", {
        "mode": args.mode, "entity": args.entity, "updates": n,
        "started_ms": started_ms, "finished_ms": finished_ms,
        "elapsed_s": elapsed, "pg_rate_per_s": n / elapsed,
        "params": vars(args),
    })
    print(f"run file: {path}")


if __name__ == "__main__":
    main()
