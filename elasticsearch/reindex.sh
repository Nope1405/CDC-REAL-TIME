#!/usr/bin/env sh
# Zero-downtime rebuild of a CDC index by replaying its Kafka topic.
#
#   reindex.sh prepare <entity> <new_version>   create <entity>_v<N> tuned for bulk load
#   reindex.sh switch  <entity> <new_version>   restore settings, swap alias atomically
#   reindex.sh status  <entity>                 show alias target and doc counts
#
# Full procedure (docs/runbook.md#rebuild-index):
#   1. reindex.sh prepare products 2
#   2. run a second consumer with a NEW group id writing to products_v2, from the
#      earliest offset (dev.ps1 rebuild products 2 / make rebuild ENTITY=products V=2)
#   3. wait until that group's lag is ~0
#   4. reindex.sh switch products 2
#   5. point the main consumer at the alias again (it already is) and stop the
#      rebuild consumer; delete the old index once satisfied.
set -eu

ES_URL="${ES_URL:-http://localhost:9200}"

usage() { sed -n '2,8p' "$0"; exit 2; }

req() {
  method="$1"; path="$2"; body="${3:-}"
  if [ -n "$body" ]; then
    printf '%s' "$body" | curl -fsS -X "$method" "$ES_URL/$path" \
      -H 'Content-Type: application/json' --data-binary @-
  else
    curl -fsS -X "$method" "$ES_URL/$path"
  fi
  echo
}

cmd="${1:-}"; entity="${2:-}"; version="${3:-}"
[ -n "$cmd" ] && [ -n "$entity" ] || usage

case "$cmd" in
  prepare)
    [ -n "$version" ] || usage
    target="${entity}_v${version}"
    # Matching index template (<entity>_v*) supplies mappings and analyzers.
    req PUT "$target" '{"settings":{"index":{"refresh_interval":"-1","number_of_replicas":0}}}'
    echo "Created $target with refresh disabled. Start the rebuild consumer now."
    ;;
  switch)
    [ -n "$version" ] || usage
    target="${entity}_v${version}"
    req PUT "$target/_settings" '{"index":{"refresh_interval":"1s"}}'
    req POST "$target/_refresh"
    current="$(curl -fsS "$ES_URL/_cat/aliases/$entity?h=index" | tr -d '[:space:]')"
    if [ -n "$current" ] && [ "$current" != "$target" ]; then
      actions="{\"remove\":{\"index\":\"$current\",\"alias\":\"$entity\"}},"
    else
      actions=""
    fi
    actions="$actions{\"add\":{\"index\":\"$target\",\"alias\":\"$entity\",\"is_write_index\":true}}"
    req POST "_aliases" "{\"actions\":[$actions]}"
    echo "Alias $entity: ${current:-<none>} -> $target"
    ;;
  status)
    curl -fsS "$ES_URL/_cat/aliases/$entity?v"
    curl -fsS "$ES_URL/_cat/indices/${entity}_v*?v&h=index,docs.count,docs.deleted,store.size&s=index"
    ;;
  *)
    usage
    ;;
esac
