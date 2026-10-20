#!/usr/bin/env sh
# Waits for Kafka Connect, then creates or updates every connector config in
# this directory. PUT /connectors/<name>/config is idempotent, so the script is
# safe to re-run after editing a JSON file.
#
# Usage: register.sh [connector-file.json ...]   (default: debezium-*.json)
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "register: waiting for $CONNECT_URL"
until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  sleep 3
done

if [ "$#" -eq 0 ]; then
  set -- "$DIR"/debezium-*.json
fi

for file in "$@"; do
  name="$(basename "$file" .json)"
  echo "register: PUT $name"
  status="$(curl -sS -o /tmp/register.out -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    --data @"$file" "$CONNECT_URL/connectors/$name/config")"
  if [ "$status" -ge 300 ]; then
    echo "register: $name failed with HTTP $status" >&2
    cat /tmp/register.out >&2
    exit 1
  fi
done

sleep 5
for file in "$@"; do
  name="$(basename "$file" .json)"
  echo "register: status $name -> $(curl -fsS "$CONNECT_URL/connectors/$name/status")"
done
