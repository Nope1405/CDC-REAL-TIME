#!/usr/bin/env bash
# Produce malformed records straight into the products topic. They must land
# in cdc.dlq.products with diagnostic headers while valid traffic keeps flowing.
. "$(dirname "$0")/lib.sh"

step "produce 3 poison records (invalid JSON, missing __lsn, base64 decimal)"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic cdc.public.products \
  --property parse.key=true --property key.separator='|' <<'EOF'
{"id":999999001}|{not json
{"id":999999002}|{"id":999999002,"name":"no lsn","__op":"u"}
{"id":999999003}|{"id":999999003,"price":"AAAAAA==","__op":"u","__lsn":1,"__source_ts_ms":1}
EOF

step "valid updates afterwards"
load per-tx --count 200

sleep 5
step "DLQ content"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic cdc.dlq.products --from-beginning \
  --property print.headers=true --property print.key=true --timeout-ms 5000 2>/dev/null | tail -3

count="$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic cdc.dlq.products 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')"
[ "$count" -ge 3 ] || fail "expected >= 3 DLQ records, found $count"
verify --entity products && ok "poison records isolated in DLQ ($count total), pipeline unaffected"
echo "Replay after fixing: python benchmark/replay_dlq.py --entity products"
