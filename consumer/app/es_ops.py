"""Translate IndexActions into Elasticsearch bulk operations.

Versioning (ADR-0002): documents are written with ``version_type=external_gte``
and the source version (PostgreSQL LSN for products, outbox aggregate version
for orders). A replayed or out-of-order older event gets HTTP 409, which the
sink treats as success.
"""

from __future__ import annotations

from typing import Any, Literal

from .events import ActionKind, IndexAction

# Partial update (TOAST placeholder present). The update API cannot use external
# versions, so the script compares against the version stored in the document.
PARTIAL_UPDATE_SCRIPT = """
def cur = ctx._source.cdc;
if (cur != null && cur.version != null && cur.version >= params.version) {
  ctx.op = 'noop';
} else {
  for (entry in params.doc.entrySet()) {
    ctx._source[entry.getKey()] = entry.getValue();
  }
}
""".strip()

# Kafka tombstone without a preceding delete in this batch (e.g. the delete
# event was already compacted away). Missing document => 404 => success.
TOMBSTONE_SCRIPT = """
if (ctx._source.is_deleted == true) {
  ctx.op = 'noop';
} else {
  ctx._source.is_deleted = true;
  if (ctx._source.cdc == null) { ctx._source.cdc = [:]; }
  ctx._source.cdc.op = 'tombstone';
  ctx._source.cdc.indexed_at = params.indexed_at;
}
""".strip()

DeleteMode = Literal["soft", "hard"]


def to_bulk_ops(
    action: IndexAction, *, index: str, delete_mode: DeleteMode, indexed_at: int
) -> list[dict[str, Any]]:
    """Return the bulk header (+ body when needed) for one action."""
    meta: dict[str, Any] = {"_index": index, "_id": action.doc_id}

    if action.kind is ActionKind.TOMBSTONE:
        if delete_mode == "hard":
            return [{"delete": meta}]
        return [
            {"update": meta},
            {"script": {"source": TOMBSTONE_SCRIPT, "lang": "painless",
                        "params": {"indexed_at": indexed_at}}},
        ]

    doc = dict(action.doc or {})
    doc["cdc"] = {**doc.get("cdc", {}), "indexed_at": indexed_at}
    versioned = {**meta, "version": action.version, "version_type": "external_gte"}

    if action.kind is ActionKind.DELETE and delete_mode == "hard":
        return [{"delete": versioned}]

    if action.kind is ActionKind.PARTIAL:
        return [
            {"update": {**meta, "retry_on_conflict": 3}},
            {
                "scripted_upsert": True,
                "upsert": {},
                "script": {
                    "source": PARTIAL_UPDATE_SCRIPT,
                    "lang": "painless",
                    "params": {"doc": doc, "version": action.version},
                },
            },
        ]

    # UPSERT, or soft DELETE (full before-image with is_deleted=true).
    if action.kind is ActionKind.DELETE:
        doc["is_deleted"] = True
    return [{"index": versioned}, doc]


def classify_item(op_type: str, item: dict[str, Any], kind: ActionKind) -> Literal["ok", "retry", "fail"]:
    """Classify one bulk response item."""
    status = int(item.get("status", 500))
    if status < 300:
        return "ok"
    if status == 409:
        return "ok"   # a newer version is already indexed
    if status == 404 and (op_type == "delete" or kind is ActionKind.TOMBSTONE):
        return "ok"   # nothing to delete
    if status == 429 or status >= 500:
        return "retry"
    return "fail"
