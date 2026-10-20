"""Seed products and orders with reproducible Vietnamese-flavoured data.

    python benchmark/seed.py --products 100000 --orders 10000
    python benchmark/seed.py --port-var POSTGRES_BASELINE_PORT --orders 0   # pgbench baseline DB

Products are loaded with COPY (fast; Debezium snapshots or streams them).
Orders go through emit_order_event() via backfill_orders_outbox(), so the
outbox contract is exercised exactly like a real service would.
"""

from __future__ import annotations

import argparse
import random
import time
from decimal import Decimal

import psycopg
from faker import Faker

from common import pg_dsn

CATEGORIES = {
    "Điện thoại": ["Điện thoại", "Smartphone"],
    "Máy tính xách tay": ["Laptop", "Máy tính xách tay"],
    "Tai nghe": ["Tai nghe không dây", "Tai nghe chụp tai"],
    "Đồng hồ": ["Đồng hồ thông minh", "Đồng hồ thể thao"],
    "Phụ kiện": ["Sạc nhanh", "Cáp sạc", "Ốp lưng", "Bàn phím cơ", "Chuột không dây"],
    "Gia dụng": ["Nồi cơm điện", "Máy lọc không khí", "Quạt điều hòa", "Bếp từ"],
}
BRANDS = ["Samsung", "Apple", "Xiaomi", "Oppo", "Sony", "Asus", "Lenovo", "Sunhouse", "Panasonic", "Logitech"]
ADJECTIVES = ["cao cấp", "chính hãng", "giá rẻ", "phiên bản mới", "màu đen", "màu trắng", "siêu bền"]
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]


def product_rows(n: int, rng: random.Random, fake: Faker):
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        noun = rng.choice(CATEGORIES[category])
        brand = rng.choice(BRANDS)
        model = f"{rng.choice('ABCXZ')}{rng.randint(1, 99)}"
        name = f"{noun} {brand} {model} {rng.choice(ADJECTIVES)}"
        # ~10% get a ~10 KB description: still > 2 KB after pglz compression, so the
        # value is stored out-of-line (TOAST). UPDATEs that do not touch it can
        # produce __debezium_unavailable_value (always without REPLICA IDENTITY
        # FULL; see chaos scenario 07 to compare).
        paragraphs = rng.randint(50, 70) if rng.random() < 0.1 else rng.randint(1, 8)
        description = " ".join(fake.paragraph(nb_sentences=6) for _ in range(paragraphs))
        price = Decimal(rng.randrange(50_000, 50_000_000, 1000)).quantize(Decimal("0.01"))
        tags = "{" + ",".join(f'"{t}"' for t in {category.lower(), brand.lower(), rng.choice(ADJECTIVES)}) + "}"
        yield (f"SKU-{i:07d}", name, description, category, brand, price,
               rng.randint(0, 500), tags, rng.random() > 0.05)


def seed_products(conn: psycopg.Connection, n: int, rng: random.Random, fake: Faker) -> None:
    t0 = time.perf_counter()
    with conn.cursor() as cur, cur.copy(
        "COPY products (sku, name, description, category, brand, price, stock, tags, is_active) FROM STDIN"
    ) as copy:
        for row in product_rows(n, rng, fake):
            copy.write_row(row)
    conn.commit()
    print(f"products: {n} rows in {time.perf_counter() - t0:.1f}s")


def seed_orders(conn: psycopg.Connection, n: int, rng: random.Random, fake: Faker) -> None:
    if n <= 0:
        return
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, price FROM products ORDER BY random() LIMIT 2000")
        catalog = cur.fetchall()
        if not catalog:
            raise SystemExit("seed products first")
        cur.execute("SELECT COALESCE(max(id), 0) FROM orders")
        start = cur.fetchone()[0]

        with cur.copy("COPY orders (id, customer_name, customer_email, status, total_amount) FROM STDIN") as copy:
            items = []
            for oid in range(start + 1, start + n + 1):
                lines = [(rng.choice(catalog), rng.randint(1, 3)) for _ in range(rng.randint(1, 5))]
                total = sum(p[2] * q for p, q in lines)
                copy.write_row((oid, fake.name(), f"khach{oid}@example.com", rng.choice(STATUSES), total))
                items.extend((oid, p[0], p[1], p[2], q) for p, q in lines)
        with cur.copy("COPY order_items (order_id, product_id, product_name, unit_price, quantity) FROM STDIN") as copy:
            for row in items:
                copy.write_row(row)
        cur.execute("SELECT setval('orders_id_seq', (SELECT max(id) FROM orders))")
        conn.commit()
        print(f"orders: {n} orders / {len(items)} items in {time.perf_counter() - t0:.1f}s")

        t1 = time.perf_counter()
        cur.execute("SELECT backfill_orders_outbox()")
        emitted = cur.fetchone()[0]
        conn.commit()
        print(f"outbox: emitted {emitted} order events in {time.perf_counter() - t1:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--products", type=int, default=100_000)
    ap.add_argument("--orders", type=int, default=10_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--truncate", action="store_true", help="TRUNCATE products/orders first")
    ap.add_argument("--port-var", default="POSTGRES_PORT", help="env var holding the PG port")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    fake = Faker("vi_VN")
    Faker.seed(args.seed)

    with psycopg.connect(pg_dsn(args.port_var)) as conn:
        if args.truncate:
            conn.execute("TRUNCATE order_items, orders, products RESTART IDENTITY CASCADE")
            conn.commit()
            print("truncated (note: TRUNCATE is not replicated as deletes; rebuild ES indices)")
        if args.products:
            seed_products(conn, args.products, rng, fake)
        seed_orders(conn, args.orders, rng, fake)


if __name__ == "__main__":
    main()
