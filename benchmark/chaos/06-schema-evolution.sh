#!/usr/bin/env bash
# Add, then drop a column on a captured table without stopping anything.
#   - Debezium picks up the new schema from the pgoutput relation message.
#   - The consumer passes unknown columns through (pydantic extra="allow").
#   - The ES mapping is dynamic:false, so the field is kept in _source but not
#     searchable until the mapping is extended (shown at the end).
. "$(dirname "$0")/lib.sh"

step "ALTER TABLE products ADD COLUMN warranty_months"
psql_c -c "ALTER TABLE products ADD COLUMN IF NOT EXISTS warranty_months integer"
psql_c -c "UPDATE products SET warranty_months = 24 WHERE id BETWEEN 1 AND 50"

sleep 5
step "field present in _source of product 1"
curl -fsS "$ES/products/_doc/1?_source_includes=id,warranty_months"; echo

step "not searchable yet (dynamic:false) -> 0 hits expected"
curl -fsS "$ES/products/_count?q=warranty_months:24"; echo

step "extend mapping in place (adding a field is allowed), then update_by_query to index it"
curl -fsS -X PUT "$ES/products/_mapping" -H 'Content-Type: application/json' \
  -d '{"properties":{"warranty_months":{"type":"integer"}}}'; echo
curl -fsS -X POST "$ES/products/_update_by_query?q=id:[1 TO 50]&refresh=true" >/dev/null
curl -fsS "$ES/products/_count?q=warranty_months:24"; echo

step "DROP COLUMN: later events simply stop carrying it"
psql_c -c "ALTER TABLE products DROP COLUMN warranty_months"
load per-tx --count 100

verify --entity products && ok "schema change handled without downtime"
echo "Remember: also add the field to elasticsearch/products-mapping.json so new index versions get it."
