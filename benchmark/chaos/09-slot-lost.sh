#!/usr/bin/env bash
# DESTRUCTIVE (dev only): force the products slot past max_slot_wal_keep_size
# so PostgreSQL invalidates it (wal_status = 'lost'), then recover.
#
# Temporarily lowers max_slot_wal_keep_size to 128MB via ALTER SYSTEM and
# restores it at the end. Requires --yes.
. "$(dirname "$0")/lib.sh"

[ "${1:-}" = "--yes" ] || { echo "This invalidates the replication slot. Re-run with --yes."; exit 2; }
trap 'psql_c -c "ALTER SYSTEM RESET max_slot_wal_keep_size" -c "SELECT pg_reload_conf()" >/dev/null' EXIT

step "lower max_slot_wal_keep_size to 128MB"
psql_c -c "ALTER SYSTEM SET max_slot_wal_keep_size = '128MB'" -c "SELECT pg_reload_conf()"

step "pause the products connector so its slot stops advancing"
curl -fsS -X PUT "$CONNECT/connectors/debezium-products/pause"
sleep 5

step "generate WAL in 24MB bursts until the paused slot is invalidated"
# Filler table is not in any publication: it only produces WAL. Bursts are
# spaced out so the running orders connector keeps confirming its LSN
# (heartbeat every 10 s + offset flush every 5 s) between bursts and stays
# well under the cap, while the paused products slot only accumulates.
psql_c -c "CREATE TABLE IF NOT EXISTS chaos_wal_filler (id bigint, pad text)" >/dev/null
state=""
for i in $(seq 1 25); do
  psql_c -c "INSERT INTO chaos_wal_filler SELECT g, repeat(md5(g::text), 6) FROM generate_series(1, 100000) g" >/dev/null
  psql_c -c "TRUNCATE chaos_wal_filler" -c "CHECKPOINT" >/dev/null
  state="$(psql_c -tAc "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'cdc_products_slot'")"
  echo "  burst $i: cdc_products_slot wal_status=$state"
  [ "$state" = "lost" ] && break
  sleep 15
done
psql_c -c "DROP TABLE chaos_wal_filler" >/dev/null
slot_status
[ "$state" = "lost" ] || fail "slot never reached wal_status=lost"
orders_state="$(psql_c -tAc "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'cdc_orders_slot'")"
[ "$orders_state" != "lost" ] || fail "the healthy orders slot was invalidated too"

step "RECOVERY (docs/runbook.md#slot-lost)"
echo "1. delete connector (its stored offset points into WAL that no longer exists)"
curl -fsS -X DELETE "$CONNECT/connectors/debezium-products"; echo
echo "2. drop the invalid slot"
psql_c -c "SELECT pg_drop_replication_slot('cdc_products_slot')" || true
echo "3. re-register under a NEW name so Connect has no stale offsets -> fresh snapshot"
# Piped via stdin: a /tmp path from Git Bash is not readable by curl on Windows.
sed 's/"name": "debezium-products"/"name": "debezium-products-r2"/' connect/debezium-products.json \
  | curl -fsS -X PUT -H 'Content-Type: application/json' --data-binary @- \
      "$CONNECT/connectors/debezium-products-r2/config" >/dev/null
sleep 10
connector_state debezium-products-r2

verify --entity products && ok "recovered from lost slot via re-snapshot"
echo "Note: the connector now runs as debezium-products-r2; rename back during a maintenance window."
