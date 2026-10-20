#!/usr/bin/env bash
# Derived data can be thrown away: rebuild products into a new index version by
# replaying the compacted topic with a fresh consumer group, then swap the
# alias atomically. Searches keep working the whole time.
#
# Usage: 08-rebuild-index.sh [new_version=2]
. "$(dirname "$0")/lib.sh"

V="${1:-2}"
GROUP="cdc-es-rebuild-products-v$V"

step "current alias"
ES_URL="$ES" sh elasticsearch/reindex.sh status products

step "prepare products_v$V"
ES_URL="$ES" sh elasticsearch/reindex.sh prepare products "$V"

step "replay cdc.public.products from offset 0 into products_v$V (group $GROUP)"
docker compose run -d --name "rebuild-products-v$V" --no-deps \
  -e KAFKA_GROUP_ID="$GROUP" -e ENTITIES=products -e ES_INDEX_PRODUCTS="products_v$V" \
  -e KAFKA_AUTO_OFFSET_RESET=earliest -e BATCH_SIZE=2000 -e METRICS_PORT=8000 \
  consumer >/dev/null

step "wait for the rebuild group to reach lag 0"
for i in $(seq 1 180); do
  lag="$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:29092 --describe --group "$GROUP" 2>/dev/null \
        | awk '/cdc\.public\.products/ {if ($6 ~ /^[0-9]+$/) s+=$6; n++} END {print (n ? s : -1)}')"
  echo "  lag=$lag"
  [ "$lag" = "0" ] && break
  sleep 5
done
[ "$lag" = "0" ] || fail "rebuild did not catch up"

step "switch alias products -> products_v$V"
ES_URL="$ES" sh elasticsearch/reindex.sh switch products "$V"

step "stop the rebuild consumer (main consumer keeps writing through the alias)"
docker rm -f "rebuild-products-v$V" >/dev/null

verify --entity products && ok "index rebuilt from the log and swapped without downtime"
echo "Old index is still there; delete it when satisfied: curl -X DELETE $ES/products_v<old>"
