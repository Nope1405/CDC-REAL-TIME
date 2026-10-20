#!/usr/bin/env bash
# Stop Elasticsearch during load. The consumer must back off, pause its
# partitions (cdc_partitions_paused=1), stay in the group, and catch up alone
# once ES is back. Nothing goes to the DLQ for a pure outage.
. "$(dirname "$0")/lib.sh"

dlq_before="$(consumer_metrics | awk '/^cdc_dlq_total/ {s+=$2} END {print s+0}')"

step "load 300 updates/s for 40 s"
load rate --rate 300 --duration 40 > /tmp/chaos-load.log 2>&1 &
LOAD=$!

sleep 8
step "stop elasticsearch for ~30 s"
docker compose stop elasticsearch
sleep 5
consumer_metrics | grep -E '^cdc_partitions_paused|^cdc_bulk_errors_total' || true
consumer_lag || true
sleep 25

step "start elasticsearch"
docker compose start elasticsearch
wait_healthy elasticsearch
wait "$LOAD"

step "verify catch-up"
verify
dlq_after="$(consumer_metrics | awk '/^cdc_dlq_total/ {s+=$2} END {print s+0}')"
[ "${dlq_after%.*}" = "${dlq_before%.*}" ] || fail "DLQ grew during an outage ($dlq_before -> $dlq_after)"
ok "consumer caught up after ES outage without DLQ traffic"
