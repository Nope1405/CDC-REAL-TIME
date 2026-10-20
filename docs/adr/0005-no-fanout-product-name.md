# ADR-0005: Order documents snapshot product names (no fan-out)

Status: accepted

## Decision

`order_items.product_name` and `unit_price` are copied at order time and
indexed as they are. Renaming a product does **not** update existing order documents.

## Why

This matches business meaning: an invoice shows what was bought, under the
name it had then. It also avoids fan-out, where one product update would
rewrite thousands of order documents, multiplying write load and lag.

## If this changes

Do it with an explicit backfill (`emit_order_event` for the affected orders),
not inside the consumer.
