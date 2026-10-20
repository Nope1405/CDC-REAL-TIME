#!/usr/bin/env bash
# Kill -9 the consumer in the middle of a load, restart it, prove no loss.
# Expectation: uncommitted batches are re-consumed (duplicates are absorbed by
# _id + external version), so PG and ES end up identical.
. "$(dirname "$0")/lib.sh"

step "start 500 updates/s for 30 s in the background"
load rate --rate 500 --duration 30 > /tmp/chaos-load.log 2>&1 &
LOAD=$!

sleep 10
step "SIGKILL consumer (no graceful flush, no commit)"
docker compose kill -s SIGKILL consumer
consumer_lag || true

sleep 10
step "restart consumer"
docker compose start consumer

wait "$LOAD"
step "consumer group lag after restart"
consumer_lag || true

step "verify PG == ES"
verify && ok "no lost or stale records after hard kill"
