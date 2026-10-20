# ADR-0002: External versioning with the source LSN

Status: accepted

## Decision

Index with `version_type=external_gte`:

- products: `version = source.lsn`, exposed by `transforms.unwrap.add.fields=lsn`.
- orders: `version = aggregate_version`, from a sequence drawn after
  `SELECT ... FOR UPDATE` on the order, so it is monotonic per order.

HTTP 409 from Elasticsearch means "a newer version is already indexed" and is treated as success.

## Why not `updated_at`

Timestamps can collide, can be set by the application, and are not
guaranteed to be monotonic. Two writes to one row are serialized by the row
lock, so the later commit always has the higher LSN.

## Why `external_gte` and not `external`

Replaying the same event is then a no-op success rather than a conflict. It
also allows an incremental snapshot chunk to re-assert the current state.

## Limits

The update API cannot take external versions. The TOAST partial-update path
therefore uses a script that compares against the stored `cdc.version`
(see `consumer/app/es_ops.py`).
