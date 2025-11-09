CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION postgres;
CREATE SCHEMA reporting AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS raw.order (
    order_id UUID PRIMARY KEY,
    order_ts TIMESTAMP NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_per_unit NUMERIC(10, 2) NOT NULL CHECK (price_per_unit >= 0),
    status TEXT NOT NULL CHECK (status IN ('completed', 'cancelled', 'refunded')),
    processed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.product (
    product_id UUID PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    processed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.customer (
    customer_id UUID PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    processed_at TIMESTAMP NOT NULL
);
