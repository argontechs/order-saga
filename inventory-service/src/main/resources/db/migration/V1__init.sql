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

CREATE TABLE stock (
    product_id VARCHAR(50) PRIMARY KEY,
    available  INT NOT NULL CHECK (available >= 0)
);

CREATE TABLE reservations (
    order_id   UUID NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity   INT NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

INSERT INTO stock (product_id, available) VALUES ('P100', 100), ('P200', 5);
