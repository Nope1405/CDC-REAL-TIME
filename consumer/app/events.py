"""Turn raw Kafka records into entity-level index actions.

Pure functions only (no Kafka/Elasticsearch clients) so every event shape can
be unit-tested from fixtures.

Product records come from Debezium + ExtractNewRecordState with
``add.fields=op,lsn,source.ts_ms,ts_ms`` and
``delete.tombstone.handling.mode=rewrite-with-tombstone``. Order records come
from the outbox EventRouter (payload = full order document, version header).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError


class EventError(Exception):
    """A record that can never be indexed (bad JSON, missing version, ...). Goes to the DLQ."""


class ActionKind(str, Enum):
    UPSERT = "upsert"           # full document, external version
    PARTIAL = "partial"         # TOAST placeholder present: merge known fields only
    DELETE = "delete"           # versioned delete (soft or hard per settings)
    TOMBSTONE = "tombstone"     # Kafka tombstone: key only, no version


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: dict[str, bytes] = field(default_factory=dict)
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class IndexAction:
    entity: str
    doc_id: str
    kind: ActionKind
    version: int | None
    doc: dict[str, Any] | None
    message: RawMessage

    @property
    def cdc(self) -> dict[str, Any]:
        return (self.doc or {}).get("cdc", {})


# --------------------------------------------------------------------------- models
class ProductRow(BaseModel):
    """Validates known columns; unknown (newly added) columns pass through untouched."""

    model_config = ConfigDict(extra="allow")

    id: int
    sku: str | None = None
    name: str | None = None
    description: str | None = None
    category: str | None = None
    brand: str | None = None
    price: Decimal | None = None
    stock: int | None = None
    tags: list[str] | None = None
    is_active: bool | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    product_id: int
    product_name: str
    unit_price: Decimal
    quantity: int


class OrderDoc(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    deleted: bool = False
    customer_name: str | None = None
    customer_email: str | None = None
    status: str | None = None
    total_amount: Decimal | None = None
    currency: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    occurred_at: datetime | None = None
    items: list[OrderItem] = []


# --------------------------------------------------------------------------- helpers
def _loads(raw: bytes | None, what: str) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventError(f"{what} is not valid JSON: {exc}") from exc


def _cdc_meta(msg: RawMessage, *, op: str, version: int, now_ms: int, **extra: Any) -> dict[str, Any]:
    meta = {
        "op": op,
        "version": version,
        "kafka_ts": msg.timestamp_ms,
        "consumed_at": now_ms,
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
    }
    meta.update({k: v for k, v in extra.items() if v is not None})
    return meta


def _key_id(msg: RawMessage) -> str:
    key = _loads(msg.key, "key")
    if isinstance(key, dict) and "id" in key:
        return str(key["id"])
    if isinstance(key, (str, int)):
        return str(key)
    raise EventError(f"cannot derive document id from key {msg.key!r}")


def _is_placeholder(value: Any, placeholder: str) -> bool:
    if value == placeholder:
        return True
    return isinstance(value, list) and len(value) > 0 and all(v == placeholder for v in value)


def _to_ms(value: datetime | None) -> int | None:
    return int(value.timestamp() * 1000) if value is not None else None


# --------------------------------------------------------------------------- products
def parse_product(msg: RawMessage, *, now_ms: int, toast_placeholder: str) -> IndexAction:
    if msg.value is None:
        return IndexAction("products", _key_id(msg), ActionKind.TOMBSTONE, None, None, msg)

    payload = _loads(msg.value, "value")
    if not isinstance(payload, dict):
        raise EventError(f"product value must be a JSON object, got {type(payload).__name__}")

    meta = {k: v for k, v in payload.items() if k.startswith("__")}
    row = {k: v for k, v in payload.items() if not k.startswith("__")}

    op = meta.get("__op")
    if op not in {"c", "u", "r", "d"}:
        raise EventError(f"missing or unknown __op {op!r}")
    lsn = meta.get("__lsn", meta.get("__source_lsn"))
    if not isinstance(lsn, int):
        raise EventError("missing __lsn: add 'lsn' to transforms.unwrap.add.fields")
    deleted = op == "d" or str(meta.get("__deleted", "false")).lower() == "true"

    unavailable = sorted(k for k, v in row.items() if _is_placeholder(v, toast_placeholder))
    for k in unavailable:
        del row[k]

    try:
        product = ProductRow.model_validate(row)
    except ValidationError as exc:
        raise EventError(f"product validation failed: {exc.errors(include_url=False)}") from exc

    doc = product.model_dump(mode="json", exclude_unset=True)
    doc["is_deleted"] = deleted
    doc["cdc"] = _cdc_meta(
        msg, op=op, version=lsn, now_ms=now_ms,
        lsn=lsn, source_ts=meta.get("__source_ts_ms"), connector_ts=meta.get("__ts_ms"),
    )

    if deleted:
        kind = ActionKind.DELETE
    elif unavailable:
        kind = ActionKind.PARTIAL
    else:
        kind = ActionKind.UPSERT
    return IndexAction("products", str(product.id), kind, lsn, doc, msg)


# --------------------------------------------------------------------------- orders
_ORDER_OPS = {"OrderCreated": "c", "OrderUpdated": "u", "OrderDeleted": "d"}


def parse_order(msg: RawMessage, *, now_ms: int, toast_placeholder: str = "") -> IndexAction:
    if msg.value is None:
        return IndexAction("orders", _key_id(msg), ActionKind.TOMBSTONE, None, None, msg)

    payload = _loads(msg.value, "value")
    if isinstance(payload, str):            # EventRouter without table.expand.json.payload
        payload = _loads(payload.encode(), "payload")
    if not isinstance(payload, dict):
        raise EventError(f"order payload must be a JSON object, got {type(payload).__name__}")

    header_version = msg.headers.get("aggregateVersion")
    try:
        version = int(header_version) if header_version is not None else int(payload["version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EventError("order event has no usable aggregateVersion/version") from exc

    try:
        order = OrderDoc.model_validate(payload)
    except ValidationError as exc:
        raise EventError(f"order validation failed: {exc.errors(include_url=False)}") from exc

    event_type = (msg.headers.get("eventType") or b"").decode() or None
    op = "d" if order.deleted else _ORDER_OPS.get(event_type or "", "u")

    doc = order.model_dump(mode="json", exclude={"deleted", "version"}, exclude_unset=True)
    doc["item_count"] = len(order.items)
    doc["is_deleted"] = order.deleted
    doc["cdc"] = _cdc_meta(msg, op=op, version=version, now_ms=now_ms,
                           source_ts=_to_ms(order.occurred_at))

    kind = ActionKind.DELETE if order.deleted else ActionKind.UPSERT
    return IndexAction("orders", str(order.id), kind, version, doc, msg)


PARSERS = {"products": parse_product, "orders": parse_order}


def parse(entity: str, msg: RawMessage, *, now_ms: int, toast_placeholder: str) -> IndexAction:
    return PARSERS[entity](msg, now_ms=now_ms, toast_placeholder=toast_placeholder)


# --------------------------------------------------------------------------- batching
def coalesce(actions: list[IndexAction]) -> list[IndexAction]:
    """Collapse several actions for the same document inside one batch.

    Input must be in partition order (same key => same partition, so relative
    order per document is preserved). Keeps the newest versioned action,
    folds PARTIAL updates into an earlier full document, and lets a
    tombstone defer to the versioned delete that precedes it.
    """
    kept: dict[tuple[str, str], IndexAction] = {}
    for action in actions:
        key = (action.entity, action.doc_id)
        prev = kept.get(key)
        if prev is None:
            kept[key] = action
            continue

        if action.kind is ActionKind.TOMBSTONE:
            if prev.kind is not ActionKind.DELETE:
                kept[key] = action
            continue
        if prev.version is not None and action.version is not None and action.version < prev.version:
            continue  # stale (replayed) event

        if action.kind is ActionKind.PARTIAL and prev.kind in (ActionKind.UPSERT, ActionKind.PARTIAL):
            merged = {**(prev.doc or {}), **(action.doc or {})}
            kept[key] = replace(action, kind=prev.kind, doc=merged)
        else:
            kept[key] = action
    return list(kept.values())
