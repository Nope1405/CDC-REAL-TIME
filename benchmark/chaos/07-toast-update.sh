#!/usr/bin/env bash
# Update products with a large (TOASTed) description WITHOUT touching it, and
# make sure Elasticsearch keeps the real description instead of the
# __debezium_unavailable_value placeholder.
#
# Run once as-is (REPLICA IDENTITY FULL) and once with --default to switch the
# table to REPLICA IDENTITY DEFAULT, which is where placeholders appear.
. "$(dirname "$0")/lib.sh"

if [ "${1:-}" = "--default" ]; then
  step "switch products to REPLICA IDENTITY DEFAULT (restored at the end)"
  psql_c -c "ALTER TABLE products REPLICA IDENTITY DEFAULT"
  trap 'psql_c -c "ALTER TABLE products REPLICA IDENTITY FULL" >/dev/null' EXIT
fi

ids="$(psql_c -tAc "SELECT string_agg(id::text, ',') FROM (SELECT id FROM products
        WHERE pg_column_size(description) > 2000 ORDER BY id LIMIT 20) t")"
[ -n "$ids" ] || fail "no TOASTed descriptions found; re-seed with benchmark/seed.py"
echo "TOASTed ids: $ids"

step "update only stock on those rows"
psql_c -c "UPDATE products SET stock = stock + 1 WHERE id IN ($ids)"
sleep 5

step "placeholder must never reach Elasticsearch"
hits="$(curl -fsS "$ES/products/_count" -H 'Content-Type: application/json' \
  -d '{"query":{"match_phrase":{"description":"__debezium_unavailable_value"}}}' | sed -E 's/.*"count":([0-9]+).*/\1/')"
[ "$hits" = "0" ] || fail "$hits documents contain the TOAST placeholder"

consumer_metrics | grep -E '^cdc_docs_indexed_total' || true
verify --entity products && ok "TOAST columns preserved"
