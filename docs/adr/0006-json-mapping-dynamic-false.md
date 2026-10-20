# ADR-0006: JSON without schemas; ES mappings `dynamic: false`

Status: accepted

## Decision

- Phase 1 uses `JsonConverter` with `schemas.enable=false`, which makes
  messages easy to read in Kafka UI. Avro with Schema Registry is deferred.
- The consumer validates known fields with pydantic (`extra="allow"`), so new
  columns pass through.
- Elasticsearch mappings are explicit, with `dynamic: false`. Unknown fields
  are stored in `_source` but not indexed, so a schema change in PostgreSQL
  never makes indexing fail.

## Consequences

- A new column becomes searchable only after a deliberate mapping update
  (in-place for an added field) or a rebuild.
- There is no schema contract enforced at the Kafka level. Moving to Avro
  later means changing the converter and moving to a new topic version.
- `dynamic: strict` was rejected: it turns every added column into DLQ traffic.
