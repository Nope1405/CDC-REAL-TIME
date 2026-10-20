#!/usr/bin/env bash
# Stop Kafka Connect and watch the replication slot retain WAL on the primary
# (the classic way CDC "fills the disk of the main database"). Restart and
# check the slot drains and the index catches up.
. "$(dirname "$0")/lib.sh"

step "slot status before"
slot_status

step "stop kafka-connect"
docker compose stop kafka-connect

step "write 20k updates while Connect is down"
load per-tx --count 20000

step "slot status: active=f, retained WAL grows"
slot_status

step "start kafka-connect"
docker compose start kafka-connect
wait_healthy kafka-connect
connector_state debezium-products

sleep 20
step "slot status after restart: retained WAL should drop back"
slot_status

verify --entity products && ok "no events lost while Connect was down"
