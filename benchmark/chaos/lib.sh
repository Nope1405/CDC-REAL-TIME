#!/usr/bin/env bash
# Shared helpers for chaos scenarios. Source it: . "$(dirname "$0")/lib.sh"
set -euo pipefail
# Git Bash on Windows rewrites "/opt/kafka/..." into "C:/Program Files/Git/opt/..."
# before passing it to `docker exec`; disable that path conversion.
export MSYS_NO_PATHCONV=1
# Any failing command (including `wait` on a crashed background load) is a FAIL.
trap 'rc=$?; printf "\033[1;31mFAIL\033[0m %s: line %s exited with %s\n" "$(basename "$0")" "$LINENO" "$rc"' ERR

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
set -a; . ./.env; set +a

CONNECT="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
ES="http://localhost:${ELASTICSEARCH_PORT:-9200}"

if [ -x .venv/Scripts/python.exe ]; then PY=.venv/Scripts/python.exe
elif [ -x .venv/bin/python ]; then PY=.venv/bin/python
else PY=python; fi

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; exit 1; }

psql_c() {
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 "$@"
}

slot_status() {
  psql_c -c "SELECT slot_name, active, wal_status,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained,
                    pg_size_pretty(safe_wal_size) AS safe_wal_size
             FROM pg_replication_slots ORDER BY slot_name"
}

consumer_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group "${CONSUMER_GROUP_ID:-cdc-es-sink}" 2>/dev/null \
    | awk 'NR==1 || /cdc\./'
}

consumer_metrics() {   # metrics of the first consumer replica (published port is dynamic)
  local addr; addr="$(docker compose port consumer 8000 2>/dev/null | head -1)"
  curl -fsS "http://${addr/0.0.0.0/localhost}/metrics" 2>/dev/null || true
}

connector_state() {
  curl -fsS "$CONNECT/connectors/$1/status"
  echo
}

load() {           # load <args for load_update.py>
  "$PY" benchmark/load_update.py "$@"
}

verify() {         # verify [--entity x] ; waits up to 180 s for catch-up
  "$PY" benchmark/verify_consistency.py --wait "${VERIFY_WAIT:-180}" "$@"
}

wait_healthy() {   # wait_healthy <service>
  local svc="$1" i
  for i in $(seq 1 90); do
    state="$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q "$svc")" 2>/dev/null || true)"
    [ "$state" = "healthy" ] && return 0
    sleep 2
  done
  fail "$svc did not become healthy"
}
