-- Transactional outbox for the orders aggregate (ADR-0001).
--
-- Write contract: every transaction that changes an order or its items MUST
-- call emit_order_event(order_id, event_type) before COMMIT. The function
-- builds the complete, denormalised order document (order + items) and writes
-- it to outbox_events in the same transaction, so the event and the state
-- change commit or roll back together.

-- Monotonic per-order version: the row lock taken in emit_order_event() is
-- held until COMMIT, so a later transaction on the same order always draws a
-- larger value. Used as the Elasticsearch external version.
CREATE SEQUENCE order_event_version_seq;

CREATE TABLE outbox_events (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type    text        NOT NULL,      -- routes to topic cdc.outbox.<aggregate_type>
    aggregate_id      text        NOT NULL,      -- Kafka key => per-order ordering
    type              text        NOT NULL,      -- OrderCreated / OrderUpdated / OrderDeleted
    aggregate_version bigint      NOT NULL,
    occurred_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload           jsonb       NOT NULL
);

CREATE FUNCTION emit_order_event(p_order_id bigint, p_type text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_version bigint;
    v_payload jsonb;
    v_event   uuid;
BEGIN
    -- Serialise concurrent writers of the same order before drawing a version.
    PERFORM 1 FROM orders WHERE id = p_order_id FOR UPDATE;
    v_version := nextval('order_event_version_seq');

    SELECT jsonb_build_object(
               'id',             o.id,
               'customer_name',  o.customer_name,
               'customer_email', o.customer_email,
               'status',         o.status,
               'total_amount',   o.total_amount::text,
               'currency',       o.currency,
               'created_at',     o.created_at,
               'updated_at',     o.updated_at,
               'deleted',        false,
               'items',          COALESCE((
                   SELECT jsonb_agg(jsonb_build_object(
                              'product_id',   i.product_id,
                              'product_name', i.product_name,
                              'unit_price',   i.unit_price::text,
                              'quantity',     i.quantity) ORDER BY i.id)
                   FROM order_items i WHERE i.order_id = o.id), '[]'::jsonb))
      INTO v_payload
      FROM orders o
     WHERE o.id = p_order_id;

    IF v_payload IS NULL THEN
        -- Order row is gone (deleted in this transaction): emit a delete marker.
        v_payload := jsonb_build_object('id', p_order_id, 'deleted', true);
        p_type := 'OrderDeleted';
    END IF;

    v_payload := v_payload || jsonb_build_object(
        'version', v_version, 'occurred_at', clock_timestamp());

    INSERT INTO outbox_events (aggregate_type, aggregate_id, type, aggregate_version, payload)
    VALUES ('order', p_order_id::text, p_type, v_version, v_payload)
    RETURNING id INTO v_event;

    -- The INSERT is already in the WAL, which is all Debezium reads. Deleting
    -- immediately keeps the table empty; the publication only publishes
    -- inserts, so the DELETE produces no event.
    DELETE FROM outbox_events WHERE id = v_event;

    RETURN v_version;
END $$;

-- Re-publish every order (initial load / index rebuild for the orders flow,
-- since the orders connector does not snapshot the outbox table).
CREATE FUNCTION backfill_orders_outbox()
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_id    bigint;
    v_count bigint := 0;
BEGIN
    FOR v_id IN SELECT id FROM orders ORDER BY id LOOP
        PERFORM emit_order_event(v_id, 'OrderUpdated');
        v_count := v_count + 1;
    END LOOP;
    RETURN v_count;
END $$;

GRANT SELECT ON outbox_events TO :"debezium_user";

CREATE PUBLICATION cdc_orders_pub
    FOR TABLE outbox_events, debezium_heartbeat
    WITH (publish = 'insert, update');
