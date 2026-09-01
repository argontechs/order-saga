CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    aggregate_id  UUID NOT NULL,
    topic         VARCHAR(100) NOT NULL,
    type          VARCHAR(255) NOT NULL,
    payload       TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    published_at  TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    id           UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id                  UUID PRIMARY KEY,
    customer_id         VARCHAR(100) NOT NULL,
    status              VARCHAR(20) NOT NULL,
    total_amount        NUMERIC(12,2) NOT NULL,
    cancellation_reason VARCHAR(255),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id   UUID NOT NULL REFERENCES orders (id),
    product_id VARCHAR(50) NOT NULL,
    quantity   INT NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL
);
