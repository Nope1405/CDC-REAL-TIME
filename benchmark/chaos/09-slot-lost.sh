#!/usr/bin/env bash
# DESTRUCTIVE (dev only): force the products slot past max_slot_wal_keep_size
# so PostgreSQL invalidates it (wal_status = 'lost'), then recover.
#
# Temporarily lowers max_slot_wal_keep_size to 64MB via ALTER SYSTEM and
# restores it at the end. Requires --yes.
. "$(dirname "$0")/lib.sh"

[ "${1:-}" = "--yes" ] || { echo "This invalidates the replication slot. Re-run with --yes."; exit 2; }
trap 'psql_c -c "ALTER SYSTEM RESET max_slot_wal_keep_size" -c "SELECT pg_reload_conf()" >/dev/null' EXIT

step "lower max_slot_wal_keep_size to 64MB"
psql_c -c "ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'" -c "SELECT pg_reload_conf()"

step "pause the products connector so its slot stops advancing"
curl -fsS -X PUT "$CONNECT/connectors/debezium-products/pause"
sleep 5

step "generate > 64MB of WAL"
for i in 1 2 3 4; do
  psql_c -c "UPDATE products SET stock = stock WHERE id % 4 = $((i - 1))" >/dev/null
  psql_c -c "CHECKPOINT" >/dev/null
done
slot_status

state="$(psql_c -tAc "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'cdc_products_slot'")"
echo "wal_status=$state"
[ "$state" = "lost" ] || echo "slot not lost yet (status=$state); generate more WAL and re-check"

step "RECOVERY (docs/runbook.md#slot-lost)"
echo "1. delete connector (its stored offset points into WAL that no longer exists)"
curl -fsS -X DELETE "$CONNECT/connectors/debezium-products"; echo
echo "2. drop the invalid slot"
psql_c -c "SELECT pg_drop_replication_slot('cdc_products_slot')" || true
echo "3. re-register under a NEW name so Connect has no stale offsets -> fresh snapshot"
sed 's/"name": "debezium-products"/"name": "debezium-products-r2"/' connect/debezium-products.json \
  > /tmp/debezium-products-r2.json
curl -fsS -X PUT -H 'Content-Type: application/json' --data @/tmp/debezium-products-r2.json \
  "$CONNECT/connectors/debezium-products-r2/config" >/dev/null
sleep 10
connector_state debezium-products-r2

verify --entity products && ok "recovered from lost slot via re-snapshot"
echo "Note: the connector now runs as debezium-products-r2; rename back during a maintenance window."
