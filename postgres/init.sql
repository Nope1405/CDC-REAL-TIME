-- Application schema, CDC support tables, least-privilege Debezium role and
-- the products publication. Executed by postgres/init.sh with psql variables
-- :debezium_user and :debezium_password.

-- ---------------------------------------------------------------------------
-- Application tables
-- ---------------------------------------------------------------------------
CREATE TABLE products (
    id          bigserial PRIMARY KEY,
    sku         text        NOT NULL UNIQUE,
    name        text        NOT NULL,
    description text,                       -- long text => TOAST candidate
    category    text        NOT NULL,
    brand       text,
    price       numeric(12, 2) NOT NULL CHECK (price >= 0),
    stock       integer     NOT NULL DEFAULT 0,
    tags        text[]      NOT NULL DEFAULT '{}',
    is_active   boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id             bigserial PRIMARY KEY,
    customer_name  text        NOT NULL,
    customer_email text        NOT NULL,
    status         text        NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    total_amount   numeric(14, 2) NOT NULL DEFAULT 0,
    currency       char(3)     NOT NULL DEFAULT 'VND',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id           bigserial PRIMARY KEY,
    order_id     bigint      NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id   bigint      NOT NULL,      -- no FK: products may be deleted later
    product_name text        NOT NULL,      -- snapshot at order time (ADR-0005: no fan-out)
    unit_price   numeric(12, 2) NOT NULL,
    quantity     integer     NOT NULL CHECK (quantity > 0)
);
CREATE INDEX order_items_order_id_idx ON order_items (order_id);

-- updated_at is maintained by the database, never trusted from the app.
CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Let callers that set updated_at explicitly (benchmark probes) keep it.
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := clock_timestamp();
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER products_set_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER orders_set_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Full before-image on UPDATE/DELETE: delete events carry every column, so a
-- soft-deleted document in Elasticsearch keeps its last known content.
ALTER TABLE products REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- Debezium support tables
-- ---------------------------------------------------------------------------
-- Written by heartbeat.action.query so each slot advances even when the
-- captured tables are idle (each connector owns one row).
CREATE TABLE debezium_heartbeat (
    id        integer PRIMARY KEY,
    connector text        NOT NULL,
    ts        timestamptz NOT NULL DEFAULT now()
);

-- Source signalling channel (incremental snapshots, see docs/runbook.md).
CREATE TABLE debezium_signal (
    id   varchar(64) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data varchar(2048)
);

-- ---------------------------------------------------------------------------
-- Least-privilege replication role
-- ---------------------------------------------------------------------------
CREATE ROLE :"debezium_user" WITH LOGIN REPLICATION PASSWORD :'debezium_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO :"debezium_user";
GRANT USAGE ON SCHEMA public TO :"debezium_user";
GRANT SELECT ON products, orders, order_items, debezium_heartbeat, debezium_signal
    TO :"debezium_user";
GRANT INSERT, UPDATE ON debezium_heartbeat TO :"debezium_user";
GRANT INSERT, UPDATE, DELETE ON debezium_signal TO :"debezium_user";

-- ---------------------------------------------------------------------------
-- Publication for the products connector (publication.autocreate.mode=disabled)
-- ---------------------------------------------------------------------------
CREATE PUBLICATION cdc_products_pub
    FOR TABLE products, debezium_heartbeat, debezium_signal;
