#!/usr/bin/env bash
# Runs once from /docker-entrypoint-initdb.d on an empty data directory.
# Passes secrets to the SQL files as psql variables instead of hardcoding them.
# No `set -u`: when this file is not executable (bind mount from Windows), the
# postgres entrypoint *sources* it and -u would leak into the entrypoint.
set -eo pipefail

: "${DEBEZIUM_DB_USER:?DEBEZIUM_DB_USER is required}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD is required}"

for f in /sql/init.sql /sql/outbox.sql; do
  echo "init: applying $f"
  psql -v ON_ERROR_STOP=1 \
       --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
       -v debezium_user="$DEBEZIUM_DB_USER" \
       -v debezium_password="$DEBEZIUM_DB_PASSWORD" \
       -f "$f"
done
