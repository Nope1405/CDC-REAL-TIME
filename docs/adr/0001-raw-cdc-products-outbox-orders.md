# ADR-0001: Raw CDC for products, transactional outbox for orders

Status: accepted

## Context

A product row maps 1:1 to a search document. An order document needs the
order row plus all of its `order_items`, which are separate tables changed in
the same transaction.

## Decision

- `products`: Debezium reads the table directly (`ExtractNewRecordState`).
- `orders`: the write path calls `emit_order_event(order_id, type)` in the same
  transaction. It builds the complete JSON document and inserts it into
  `outbox_events`. Debezium's `EventRouter` routes it to `cdc.outbox.order`,
  keyed by order id. The row is deleted immediately (the publication only
  publishes inserts), so the table stays empty.

## Consequences

- The consumer never joins tables and never queries the primary.
- Event payloads are a contract owned by the database function. Changing the
  document shape means changing `emit_order_event()`.
- Any write path that forgets to call `emit_order_event()` produces no event.
  This is the main risk, mitigated by `verify_consistency.py --entity orders`.
- The orders connector has nothing to snapshot. History is loaded with
  `backfill_orders_outbox()`.
