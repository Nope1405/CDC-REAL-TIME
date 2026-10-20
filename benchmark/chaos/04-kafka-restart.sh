#!/usr/bin/env bash
# Restart the (single) Kafka broker under load. Debezium and the consumer must
# both reconnect by themselves; Debezium resumes from its committed offset/LSN.
. "$(dirname "$0")/lib.sh"

step "load 200 updates/s for 45 s"
load rate --rate 200 --duration 45 > /tmp/chaos-load.log 2>&1 &
LOAD=$!

sleep 10
step "restart kafka"
docker compose restart kafka
wait_healthy kafka

step "connector state (should be RUNNING; restart failed tasks if not)"
connector_state debezium-products
connector_state debezium-orders-outbox

wait "$LOAD"
verify && ok "pipeline recovered from broker restart"
