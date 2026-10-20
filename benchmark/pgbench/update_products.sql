-- pgbench custom script: one single-row UPDATE on the CDC-captured table.
-- Run with: pgbench -n -f /bench/update_products.sql -D max_id=100000 ...
\set id random(1, :max_id)
UPDATE products SET price = price + 1, stock = (stock + 1) % 1000 WHERE id = :id;
