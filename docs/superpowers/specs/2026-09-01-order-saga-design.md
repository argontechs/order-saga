# Order Saga — Event-Driven Order Processing System

**Date:** 2026-09-01
**Status:** Approved
**Purpose:** Portfolio project for Java backend job applications. Demonstrates event-driven architecture with Kafka: choreography saga, transactional outbox, idempotent consumers, dead-letter topics, CQRS read model with Kafka Streams, and full observability.

## Goals

- Showcase idiomatic Spring Boot + Kafka patterns that Java backend interviews test.
- Every phase leaves a shippable, well-documented repo (abandonment insurance).
- Recruiter-readable: strong README, clean git history, CI badge, diagrams, screenshots.

## Non-Goals

- Real payment integration (fake PSP with configurable failure rate).
- Authentication/authorization.
- Kubernetes deployment (Docker Compose only; README may note the upgrade path).
- Debezium CDC (polling outbox publisher instead; README notes Debezium as the production upgrade path).

## Architecture

**Approach:** Choreography saga (services react to each other's events, no central orchestrator) plus a Kafka Streams query side (CQRS). Order service starts the saga and projects final state; payment, inventory, and shipping each listen and emit results; compensation flows through events.

### Stack

| Concern | Choice |
|---|---|
| Language / runtime | Java 21 |
| Framework | Spring Boot 3.5, Spring Kafka, Kafka Streams |
| Build | Maven multi-module monorepo |
| Broker | Apache Kafka, KRaft mode (no ZooKeeper), single broker |
| Storage | Postgres — one container, separate database per service |
| Schemas | Confluent Schema Registry + Avro (phase 2; JSON before that) |
| Local infra | Docker Compose |
| Tests | JUnit 5, Mockito, Testcontainers |
| CI | GitHub Actions |
| Observability | Micrometer + OpenTelemetry → Jaeger; Prometheus + Grafana (phase 4) |

### Modules

```
order-saga/                     (repo root, Maven parent)
├── common-events/              shared event schemas (JSON records first, Avro in phase 2)
├── order-service/              REST API: create order, query status. Owns saga start + final state
├── payment-service/            fake PSP, configurable failure rate (env var)
├── inventory-service/          stock reserve/release
├── shipping-service/           shipment creation
├── order-view-service/         Kafka Streams read model (phase 3): order timeline, CQRS query API
└── docker-compose.yml          kafka, postgres, schema-registry, observability stack
```

Each saga service (order, payment, inventory, shipping) owns:
- Its own Postgres database.
- A transactional **outbox** table; a polling outbox publisher moves rows to Kafka.
- A **processed_events** table (event id as primary key) for idempotent consumption.

order-view-service has no Postgres database — its state lives in Kafka Streams state stores (backed by changelog topics).

### Topics

`orders.events`, `payments.events`, `inventory.events`, `shipping.events`, plus one `<consumer-group>.DLT` dead-letter topic per consumer. All events keyed by `orderId` — per-order ordering guaranteed within a partition.

## Data Flow

### Happy path

1. `POST /orders` → order-service writes order (status `PENDING`) and an `OrderCreated` row to its outbox in the same DB transaction. Outbox publisher pushes the event to `orders.events`.
2. payment-service consumes `OrderCreated` → authorizes via fake PSP → emits `PaymentAuthorized`.
3. inventory-service consumes `PaymentAuthorized` → reserves stock → emits `InventoryReserved`.
4. shipping-service consumes `InventoryReserved` → creates shipment → emits `OrderShipped`.
5. order-service consumes `PaymentAuthorized` / `InventoryReserved` / `OrderShipped` and advances order status to `CONFIRMED`.

### Failure paths (compensation)

| Failure | Compensation events | Outcome |
|---|---|---|
| Payment declined | `PaymentFailed` | nothing to compensate |
| Out of stock | `OutOfStock` → payment-service refunds → `PaymentRefunded` | stock never reserved |
| Shipping failed | `ShipmentFailed` → inventory releases stock (`InventoryReleased`), payment refunds (`PaymentRefunded`) | both compensated |

order-service marks the order `CANCELLED` on the **terminal failure event** (`PaymentFailed`, `OutOfStock`, or `ShipmentFailed`); refund/release events are compensation side effects, not the cancellation trigger. Compensation events are emitted to the compensating service's own topic (`PaymentRefunded` → `payments.events`, `InventoryReleased` → `inventory.events`).

### Event catalog

`OrderCreated`, `PaymentAuthorized`, `PaymentFailed`, `PaymentRefunded`, `InventoryReserved`, `OutOfStock`, `InventoryReleased`, `OrderShipped`, `ShipmentFailed`.

Every event carries: `eventId` (UUID), `orderId` (partition key), `occurredAt`, type-specific payload.

## Error Handling

- **Retries / poison pills:** Spring Kafka `DefaultErrorHandler`, exponential backoff, 3 attempts, then publish to `<consumer-group>.DLT` with failure-reason headers.
- **Idempotency:** every consumer inserts into `processed_events` (eventId PK) inside the same transaction as its business write; duplicate delivery is a no-op. Covers Kafka's at-least-once delivery.
- **Demo lever:** fake PSP failure rate configurable via env var so compensation can be demonstrated live.

## Read Side (CQRS, phase 3)

order-view-service runs a Kafka Streams topology that consumes all four event topics, groups by `orderId`, and materializes a state store holding the full event timeline and current status per order. Exposed via interactive queries: `GET /orders/{id}/timeline`.

## Testing

- **Unit** (JUnit 5 + Mockito): saga state transitions, outbox publisher, compensation logic.
- **Integration per service** (Testcontainers: Kafka + Postgres): consume a real event, assert DB state and emitted event. Must include an idempotency test (same event delivered twice) and a DLT test (poison message lands in DLT).
- **End-to-end** (Testcontainers, full system): place an order and assert `CONFIRMED`; place an order with forced payment failure and assert `CANCELLED`.

## Observability (phase 4)

- Micrometer + OpenTelemetry tracing → Jaeger: a single order's trace spans all four services across Kafka hops. Screenshot in README.
- Prometheus + Grafana: consumer lag, processing rate, DLT counts. Provisioned dashboard JSON committed to the repo.

## Repo Polish

- README: Mermaid architecture + saga sequence diagrams, pattern write-ups (outbox, idempotency, DLT, CQRS), quickstart (`docker compose up`), screenshots.
- GitHub Actions: build + all tests on push.
- Conventional commits, small atomic history.
- Commits authored by the repo owner only — no AI co-author trailers.

## Phasing

Each phase ends with a shippable repo: green CI, updated README.

1. **Core saga** — 4 services + Kafka + outbox + idempotency + DLT + Testcontainers + CI + README.
2. **Schema Registry + Avro** — migrate events from JSON to Avro, add registry to compose.
3. **Streams read model** — order-view-service with interactive queries.
4. **Observability** — OpenTelemetry/Jaeger tracing, Prometheus + Grafana dashboards.
