#!/usr/bin/env bash
# Does CDC slow the primary down? Runs the same pgbench workload in 3 setups:
#
#   baseline   postgres-baseline container: same image/config, no replication slot
#   cdc        main postgres with both connectors streaming
#   paused     main postgres with the products connector paused (slot retains WAL)
#
# Prerequisites: core stack up and seeded; then
#   docker compose --profile bench up -d postgres-baseline
#   python benchmark/seed.py --port-var POSTGRES_BASELINE_PORT --orders 0
#
# Usage: benchmark/pgbench/compare.sh [duration_s=60] [clients=8]
set -euo pipefail
# Git Bash on Windows rewrites "/opt/kafka/..." into "C:/Program Files/Git/opt/..."
# before passing it to `docker exec`; disable that path conversion.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a

DURATION="${1:-60}"
CLIENTS="${2:-8}"
CONNECT="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
OUT="results/$(date +%Y%m%d-%H%M%S)-pgbench"
mkdir -p "$OUT"

max_id() {
  docker compose exec -T "$1" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT max(id) FROM products"
}

run() {
  local label="$1" service="$2"
  local max; max="$(max_id "$service" | tr -d '[:space:]')"
  echo "== $label ($service, max_id=$max, ${DURATION}s, ${CLIENTS} clients)"
  docker compose exec -T "$service" pgbench -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -n -f /bench/update_products.sql -D "max_id=$max" \
    -c "$CLIENTS" -j "$CLIENTS" -T "$DURATION" -P 10 --report-per-command \
    | tee "$OUT/$label.txt"
}

slot_lag() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag, wal_status FROM pg_replication_slots"
}

run baseline postgres-baseline
run cdc postgres

echo "== pausing debezium-products"
curl -fsS -X PUT "$CONNECT/connectors/debezium-products/pause"
sleep 5
run paused postgres
slot_lag | tee "$OUT/slot-after-paused.txt"
curl -fsS -X PUT "$CONNECT/connectors/debezium-products/resume"
echo "== resumed debezium-products"

echo
echo "Summary (tps / latency average):"
for f in "$OUT"/baseline.txt "$OUT"/cdc.txt "$OUT"/paused.txt; do
  printf '%-10s %s | %s\n' "$(basename "$f" .txt)" \
    "$(grep -E '^tps' "$f" | head -1)" "$(grep -E '^latency average' "$f")"
done
echo "Raw output: $OUT/"
