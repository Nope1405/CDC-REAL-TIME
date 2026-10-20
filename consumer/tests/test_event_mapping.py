"""Debezium/outbox record -> IndexAction -> Elasticsearch bulk op."""

import json
from dataclasses import replace

import pytest

from app.es_ops import PARTIAL_UPDATE_SCRIPT, TOMBSTONE_SCRIPT, classify_item, to_bulk_ops
from app.events import ActionKind, EventError, coalesce, parse_order, parse_product

NOW = 1_792_483_001_000
PH = "__debezium_unavailable_value"


def product(raw, name):
    return parse_product(raw(name), now_ms=NOW, toast_placeholder=PH)


def order(raw, name):
    return parse_order(raw(name), now_ms=NOW)


# --------------------------------------------------------------------------- products
@pytest.mark.parametrize("name,op", [("product_snapshot", "r"), ("product_create", "c"), ("product_update", "u")])
def test_snapshot_create_update_become_versioned_upserts(raw, name, op):
    a = product(raw, name)
    assert a.kind is ActionKind.UPSERT
    assert a.doc_id == "42"
    assert a.version == a.doc["cdc"]["lsn"]
    assert a.doc["cdc"]["op"] == op
    assert a.doc["is_deleted"] is False
    assert not any(k.startswith("__") for k in a.doc)


def test_decimal_stays_exact_string(raw):
    a = product(raw, "product_update")
    assert a.doc["price"] == "8490000.00"


def test_cdc_timestamps_for_every_stage(raw):
    cdc = product(raw, "product_create").doc["cdc"]
    assert cdc["source_ts"] <= cdc["connector_ts"] <= cdc["kafka_ts"] <= cdc["consumed_at"] == NOW
    assert (cdc["topic"], cdc["partition"], cdc["offset"]) == ("cdc.public.products", 3, 10)


def test_vietnamese_text_preserved(raw):
    assert product(raw, "product_create").doc["name"] == "Điện thoại Samsung Galaxy A55"


def test_toast_placeholder_becomes_partial_update_without_that_field(raw):
    a = product(raw, "product_toast_update")
    assert a.kind is ActionKind.PARTIAL
    assert "description" not in a.doc
    assert a.doc["stock"] == 15
    assert PH not in json.dumps(a.doc)


def test_rewritten_delete_keeps_before_image(raw):
    a = product(raw, "product_delete")
    assert a.kind is ActionKind.DELETE
    assert a.doc["is_deleted"] is True
    assert a.doc["name"]


def test_tombstone_uses_key(raw):
    a = product(raw, "product_tombstone")
    assert a.kind is ActionKind.TOMBSTONE
    assert a.doc_id == "42" and a.version is None


def test_unknown_column_passes_through(raw):
    a = product(raw, "product_new_column")
    assert a.doc["warranty_months"] == 24


def test_base64_decimal_is_rejected(raw):
    with pytest.raises(EventError, match="price"):
        product(raw, "product_bad_price")


def test_missing_lsn_is_rejected(raw):
    msg = raw("product_update")
    value = json.loads(msg.value)
    del value["__lsn"]
    with pytest.raises(EventError, match="lsn"):
        parse_product(replace(msg, value=json.dumps(value).encode()), now_ms=NOW, toast_placeholder=PH)


def test_invalid_json_is_rejected(raw):
    with pytest.raises(EventError, match="JSON"):
        parse_product(replace(raw("product_update"), value=b"{oops"), now_ms=NOW, toast_placeholder=PH)


# --------------------------------------------------------------------------- orders
def test_outbox_order_uses_header_version_and_items(raw):
    a = order(raw, "order_created")
    assert a.kind is ActionKind.UPSERT
    assert (a.doc_id, a.version) == ("1001", 77)
    assert a.doc["item_count"] == 1
    assert a.doc["items"][0]["unit_price"] == "8990000.00"
    assert a.doc["cdc"]["op"] == "c"
    assert a.doc["cdc"]["source_ts"] == 1_792_483_200_120
    assert "deleted" not in a.doc and "version" not in a.doc


def test_outbox_order_delete(raw):
    a = order(raw, "order_deleted")
    assert a.kind is ActionKind.DELETE
    assert a.version == 78
    assert a.doc["is_deleted"] is True


def test_outbox_payload_as_json_string(raw):
    msg = raw("order_created")
    double_encoded = json.dumps(msg.value.decode()).encode()
    a = parse_order(replace(msg, value=double_encoded), now_ms=NOW)
    assert a.doc_id == "1001"


def test_order_version_falls_back_to_payload(raw):
    a = parse_order(replace(raw("order_created"), headers={}), now_ms=NOW)
    assert a.version == 77


# --------------------------------------------------------------------------- bulk ops
def ops(a, mode="soft"):
    return to_bulk_ops(a, index="products", delete_mode=mode, indexed_at=NOW + 5)


def test_upsert_is_external_gte_index(raw):
    header, body = ops(product(raw, "product_update"))
    assert header["index"]["version_type"] == "external_gte"
    assert header["index"]["version"] == 23457000
    assert header["index"]["_id"] == "42"
    assert body["cdc"]["indexed_at"] == NOW + 5


def test_soft_delete_indexes_tombstone_document(raw):
    header, body = ops(product(raw, "product_delete"), "soft")
    assert "index" in header and body["is_deleted"] is True


def test_hard_delete_is_versioned_delete(raw):
    (header,) = ops(product(raw, "product_delete"), "hard")
    assert header["delete"]["version"] == 23459000


def test_partial_uses_guarded_script(raw):
    header, body = ops(product(raw, "product_toast_update"))
    assert "update" in header
    assert body["script"]["source"] == PARTIAL_UPDATE_SCRIPT
    assert body["script"]["params"]["version"] == 23458000
    assert body["scripted_upsert"] is True


def test_tombstone_ops(raw):
    header, body = ops(product(raw, "product_tombstone"), "soft")
    assert "update" in header and body["script"]["source"] == TOMBSTONE_SCRIPT
    assert ops(product(raw, "product_tombstone"), "hard") == [{"delete": {"_index": "products", "_id": "42"}}]


@pytest.mark.parametrize("op_type,status,kind,expected", [
    ("index", 201, ActionKind.UPSERT, "ok"),
    ("index", 409, ActionKind.UPSERT, "ok"),
    ("delete", 404, ActionKind.DELETE, "ok"),
    ("update", 404, ActionKind.TOMBSTONE, "ok"),
    ("index", 429, ActionKind.UPSERT, "retry"),
    ("index", 503, ActionKind.UPSERT, "retry"),
    ("index", 400, ActionKind.UPSERT, "fail"),
    ("update", 404, ActionKind.PARTIAL, "fail"),
])
def test_classify_item(op_type, status, kind, expected):
    assert classify_item(op_type, {"status": status}, kind) == expected


# --------------------------------------------------------------------------- coalescing
def test_coalesce_keeps_latest_version(raw):
    out = coalesce([product(raw, "product_create"), product(raw, "product_update")])
    assert len(out) == 1 and out[0].version == 23457000


def test_coalesce_drops_stale_replay(raw):
    out = coalesce([product(raw, "product_update"), product(raw, "product_create")])
    assert out[0].version == 23457000


def test_coalesce_folds_partial_into_full_doc(raw):
    out = coalesce([product(raw, "product_update"), product(raw, "product_toast_update")])
    (a,) = out
    assert a.kind is ActionKind.UPSERT
    assert a.version == 23458000
    assert a.doc["description"] == "Màn hình 6.6 inch, pin 5000mAh"   # from the full doc
    assert a.doc["stock"] == 15                                         # from the partial


def test_coalesce_tombstone_defers_to_delete(raw):
    (a,) = coalesce([product(raw, "product_update"), product(raw, "product_delete"),
                     product(raw, "product_tombstone")])
    assert a.kind is ActionKind.DELETE and a.version == 23459000


def test_coalesce_keeps_documents_separate(raw):
    other = replace(product(raw, "product_update"), doc_id="43")
    assert len(coalesce([product(raw, "product_update"), other])) == 2
