# ADR-0003: Soft deletes by default

Status: accepted

## Context

Elasticsearch remembers the version of a hard-deleted document only for
`index.gc_deletes` (60 s). If an older create or update for that id arrives
later (replay, DLQ replay, rebuild overlap), it recreates the document.

## Decision

`DELETE_MODE=soft` (default): a delete indexes the before-image with
`is_deleted=true` and the delete event's version. Queries filter
`is_deleted:false`. `DELETE_MODE=hard` stays available (a versioned `delete`)
for comparison.

## Consequences

- Deletes are replay-safe forever.
- The index keeps deleted documents. Purge them periodically with
  `_delete_by_query` on `is_deleted:true AND cdc.indexed_at < now-7d`, or let
  the next rebuild drop them.
- Every search must filter on `is_deleted`. Use a filtered alias if clients
  cannot be trusted to add the filter.
