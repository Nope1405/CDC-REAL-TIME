# Architecture decision records

| ADR | Decision |
| --- | --- |
| [0001](0001-raw-cdc-products-outbox-orders.md) | Raw CDC for products, transactional outbox for orders |
| [0002](0002-external-versioning-lsn.md) | `external_gte` versioning with LSN / aggregate version |
| [0003](0003-soft-delete.md) | Soft deletes by default (replay-safe) |
| [0004](0004-fixed-partitions-compaction.md) | Fixed partition counts, compacted CDC topics |
| [0005](0005-no-fanout-product-name.md) | Orders snapshot product names; no fan-out |
| [0006](0006-json-mapping-dynamic-false.md) | JSON without schemas; ES `dynamic: false` |

New ADRs use the next number and the headings Status / Context / Decision / Consequences.
