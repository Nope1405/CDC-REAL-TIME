#!/usr/bin/env sh
# Installs the component/index templates and creates <entity>_v1 behind the
# <entity> alias when the alias does not exist yet. Idempotent; runs in the
# one-shot es-init container and can be re-run after editing a mapping
# (templates only affect indices created afterwards - see reindex.sh).
set -eu

ES_URL="${ES_URL:-http://elasticsearch:9200}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "es-init: waiting for $ES_URL"
until curl -fsS "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
  sleep 3
done

put() {
  path="$1"; body="$2"
  printf '%s' "$body" | curl -fsS -X PUT "$ES_URL/$path" \
    -H 'Content-Type: application/json' --data-binary @- >/dev/null
  echo "es-init: PUT $path"
}

put "_component_template/cdc-common" "$(cat "$DIR/index-template.json")"

for entity in products orders; do
  template="$(printf '{"index_patterns":["%s_v*"],"priority":200,"composed_of":["cdc-common"],"template":%s}' \
    "$entity" "$(cat "$DIR/$entity-mapping.json")")"
  put "_index_template/$entity" "$template"

  if curl -fsS -o /dev/null "$ES_URL/_alias/$entity" 2>/dev/null; then
    echo "es-init: alias $entity already exists -> $(curl -fsS "$ES_URL/_cat/aliases/$entity?h=index")"
  else
    put "${entity}_v1" "{\"aliases\":{\"$entity\":{\"is_write_index\":true}}}"
  fi
done

echo "es-init: done"
