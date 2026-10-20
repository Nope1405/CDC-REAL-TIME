# Debezium fixtures

Each file is one Kafka record as the consumer receives it, after Debezium's
JSON converter (`schemas.enable=false`) and SMTs:

```json
{ "topic": "...", "partition": 0, "offset": 0,
  "key": <JSON or null>, "value": <JSON or null>,
  "headers": {"name": "string"}, "timestamp_ms": 0 }
```

| File | Shape it covers |
| --- | --- |
| `product_snapshot.json` | `__op=r` from the initial snapshot |
| `product_create.json` / `product_update.json` | `c` / `u` after `ExtractNewRecordState` with `add.fields=op,lsn,source.ts_ms,ts_ms` |
| `product_toast_update.json` | unchanged TOAST column arrives as `__debezium_unavailable_value` |
| `product_delete.json` | delete rewritten by `rewrite-with-tombstone` (`__deleted="true"`, before-image) |
| `product_tombstone.json` | the Kafka tombstone (value `null`) that follows a delete |
| `product_new_column.json` | schema evolution: a column the consumer does not know |
| `product_bad_price.json` | `NUMERIC` encoded as base64 bytes (forgot `decimal.handling.mode=string`) |
| `order_created.json` / `order_deleted.json` | outbox `EventRouter` output with `eventType` / `aggregateVersion` headers |

To capture a real record and add a fixture, read from the topic with
`kafka-console-consumer.sh --property print.key=true --property print.headers=true`
(or copy it from Kafka UI) and wrap it in the shape above.
