# Phase 4: Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Distributed tracing (Micrometer Tracing → OTLP → Jaeger) across the whole saga — including trace continuity through the outbox hop — plus Prometheus metrics and a provisioned Grafana dashboard.

**Architecture:** Every service gets Micrometer Tracing with the OTel bridge and OTLP export to Jaeger. Spring Kafka observations are enabled on templates and listener containers, so producer/consumer spans join traces via `traceparent` record headers. The outbox would normally break the trace (the polling publisher runs on a scheduler thread, not the request thread) — so `OutboxWriter` captures the current W3C `traceparent` into a new outbox column, and the publisher replays it as a record header at send time; consumer-side observation extracts it and the whole saga renders as ONE trace in Jaeger. Metrics: actuator + Prometheus registry per service, a DLT counter wrapped around the recoverer, Prometheus scraping all services inside the compose network, Grafana with provisioned datasource + dashboard JSON. Jaeger joins the base compose infra (tiny, keeps CI logs clean); Prometheus + Grafana live under a new `obs` profile so the CI e2e stack is unchanged.

**Tech Stack additions:** micrometer-tracing-bridge-otel, opentelemetry-exporter-otlp, spring-boot-starter-actuator, micrometer-registry-prometheus, jaegertracing/all-in-one, prom/prometheus, grafana/grafana.

**Spec:** `docs/superpowers/specs/2026-09-01-order-saga-design.md` (Phase 4 row + Observability section; this plan implements only that).

## Global Constraints

- Everything from Phases 1-3 binds: Java 21, Spring Boot 3.5.x, base package `dev.argontechs.ordersaga`, user-identity commits with NO AI co-author trailer, conventional commits, main branch.
- Tracing config identical across all 5 services (order, payment, inventory, shipping, order-view): `management.tracing.sampling.probability: 1.0`, `management.otlp.tracing.endpoint: http://localhost:4318/v1/traces` (compose env override `MANAGEMENT_OTLP_TRACING_ENDPOINT: http://jaeger:4318/v1/traces`), `spring.kafka.template.observation-enabled: true` (the 4 saga services), `spring.kafka.listener.observation-enabled: true` (all consumers).
- Outbox trace continuity: new nullable `traceparent VARCHAR(64)` column (Flyway V2 migration in each of the 4 saga services); `OutboxWriter` captures it via Micrometer `Propagator.inject` when a tracer is present (no-op otherwise — unit tests and non-traced contexts must keep working); `OutboxPublisher` sets it as the `traceparent` record header when non-null.
- Ports: Jaeger UI 16686, OTLP 4318 (http); Prometheus 9090; Grafana 3000 (admin/admin, anonymous viewer enabled is fine for local demo). Jaeger = base compose service with healthcheck; prometheus + grafana under `profiles: [obs]`.
- Actuator exposure: `health,prometheus` only.
- DLT metric: counter `ordersaga.dlt.messages` (tag `group`) incremented in KafkaErrorConfig's recoverer wrapper before delegating to the DeadLetterPublishingRecoverer.
- CI must stay green with NO workflow changes (obs profile keeps prometheus/grafana out of the e2e stack; jaeger in base infra is cheap and silences OTLP export warnings).
- Every task ends with the touched modules' tests green; final task ends with full reactor + composed e2e green and pushed.

---

### Task 1: Distributed tracing — deps, config, outbox traceparent, Jaeger, verified end-to-end

**Files:**
- Modify: `pom.xml` (root — dependencyManagement not needed; boot manages micrometer/otel versions)
- Modify: all 5 service poms (+ common-messaging pom)
- Modify: all 5 `application.yml`
- Modify: `common-messaging`: `OutboxMessage.java` (traceparent field), `OutboxWriter.java`, `OutboxPublisher.java`
- Create: `*/src/main/resources/db/migration/V2__outbox_traceparent.sql` (×4 saga services)
- Modify: `docker-compose.yml` (jaeger base service + `MANAGEMENT_OTLP_TRACING_ENDPOINT` env on all 5 app services)
- Test: `common-messaging/src/test/java/dev/argontechs/ordersaga/messaging/OutboxTraceparentTest.java`

**Interfaces:**
- Produces: outbox rows carry nullable `traceparent`; publisher emits it as the `traceparent` header; every service exports OTLP spans. Task 3's Jaeger screenshot/API verification depends on this.

- [ ] **Step 1: Dependencies**

Each of the 5 service poms gains:

```xml
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-tracing-bridge-otel</artifactId>
    </dependency>
    <dependency>
      <groupId>io.opentelemetry</groupId>
      <artifactId>opentelemetry-exporter-otlp</artifactId>
    </dependency>
```

`common-messaging/pom.xml` gains (for the Propagator API only):

```xml
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-tracing</artifactId>
    </dependency>
```

- [ ] **Step 2: Config (all 5 application.yml)**

Append:

```yaml
management:
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: http://localhost:4318/v1/traces
  endpoints:
    web:
      exposure:
        include: health,prometheus
```

And under the existing `spring.kafka` block: `template.observation-enabled: true` (the 4 saga services — order-view has no template) and `listener.observation-enabled: true` (payment, inventory, shipping, order — order-view's Streams app has no listener container; skip it there).

- [ ] **Step 3: Outbox traceparent**

`V2__outbox_traceparent.sql` (identical ×4):

```sql
ALTER TABLE outbox ADD COLUMN traceparent VARCHAR(64);
```

`OutboxMessage`: add nullable `traceparent` field + getter + `setTraceparent(String)`.

`OutboxWriter`: inject `ObjectProvider<io.micrometer.tracing.propagation.Propagator>` and `ObjectProvider<io.micrometer.tracing.Tracer>`; after building the row, when both beans are present and there is a current span, capture:

```java
        var tracer = tracerProvider.getIfAvailable();
        var propagator = propagatorProvider.getIfAvailable();
        if (tracer != null && propagator != null && tracer.currentSpan() != null) {
            var carrier = new java.util.HashMap<String, String>();
            propagator.inject(tracer.currentTraceContext().context(), carrier, Map::put);
            row.setTraceparent(carrier.get("traceparent"));
        }
```

`OutboxPublisher`: when `row.getTraceparent() != null`, send a `ProducerRecord` with a `traceparent` header (UTF-8 bytes) instead of the bare `send(topic, key, value)`; otherwise unchanged. (Consumer-side spring-kafka observation extracts the header and parents the consumer span onto the original trace — that is the whole continuity mechanism; keep a short comment saying so.)

- [ ] **Step 4: Unit test**

`OutboxTraceparentTest`: with mocked Tracer/Propagator (propagator's inject puts a fixed `traceparent` value), `OutboxWriter.write` stores it on the row; with empty ObjectProviders, `traceparent` stays null and write still succeeds. Publisher: a row with traceparent → the ProducerRecord sent carries the header; without → no header. (Mockito, same style as the existing tests in that module.)

- [ ] **Step 5: Compose**

Base service:

```yaml
  jaeger:
    image: jaegertracing/all-in-one:1.60
    ports:
      - "16686:16686"
      - "4318:4318"
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

(No healthcheck — the all-in-one image may lack wget/curl, and services tolerate a late collector: OTLP export just retries with warnings.)

All 5 app services: add env `MANAGEMENT_OTLP_TRACING_ENDPOINT: http://jaeger:4318/v1/traces` and `jaeger: condition: service_started` to depends_on.

- [ ] **Step 6: Verify end-to-end (the money check)**

```bash
mvn -B verify                       # all module tests incl. new unit tests
mvn -DskipTests package
docker compose --profile app up --build -d
# place a happy-path order:
ORDER=$(curl -s -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"trace-demo","items":[{"productId":"P100","quantity":1,"unitPrice":49.90}]}' | jq -r .orderId)
sleep 20
curl -s "http://localhost:16686/api/services" | jq .
# assert all 5 services report; then fetch the trace and confirm it spans multiple services:
curl -s "http://localhost:16686/api/traces?service=order-service&limit=5" | jq '[.data[].processes[].serviceName] | unique'
docker compose --profile app down
```

Expected: services list contains order/payment/inventory/shipping/order-view; at least one trace's process list contains ≥3 different saga services (that proves the outbox traceparent hop works — without it every service would start its own root trace). Include the jq output in the report.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: distributed tracing with trace continuity through the outbox"
```

---

### Task 2: Metrics — Prometheus registry, DLT counter, Prometheus + Grafana provisioning

**Files:**
- Modify: all 5 service poms (+ `micrometer-registry-prometheus`)
- Modify: `common-messaging/pom.xml` (micrometer-core) + `KafkaErrorConfig.java` (DLT counter)
- Create: `infra/prometheus.yml`
- Create: `infra/grafana/provisioning/datasources/prometheus.yml`
- Create: `infra/grafana/provisioning/dashboards/dashboards.yml`
- Create: `infra/grafana/provisioning/dashboards/order-saga.json`
- Modify: `docker-compose.yml` (prometheus + grafana under `profiles: [obs]`)
- Test: extend `common-messaging` tests: DLT counter increments on recovery.

**Interfaces:**
- Produces: `/actuator/prometheus` on all 5 services; counter `ordersaga.dlt.messages{group=...}`; `docker compose --profile app --profile obs up` = full observability stack.

- [ ] **Step 1: Deps** — each service pom + `micrometer-registry-prometheus`; common-messaging + `micrometer-core`.
- [ ] **Step 2: DLT counter** — `KafkaErrorConfig.kafkaErrorHandler` gains a `MeterRegistry` parameter; wrap the recoverer:

```java
        var dlqRecoverer = new DeadLetterPublishingRecoverer(templates, destinationResolver);
        ConsumerRecordRecoverer counted = (rec, ex) -> {
            registry.counter("ordersaga.dlt.messages", "group", group).increment();
            dlqRecoverer.accept(rec, ex);
        };
        return new DefaultErrorHandler(counted, backOff);
```

Unit test: invoke the recoverer path (call the handler's recoverer or construct the lambda equivalently) with a SimpleMeterRegistry and assert the counter incremented. Keep it honest — test through `KafkaErrorConfig`'s public surface if reachable; otherwise extract the counting recoverer into a small package-private factory method and test that.

- [ ] **Step 3: Prometheus config** — `infra/prometheus.yml`: one scrape job, 5s interval, static targets `order-service:8081`, `payment-service:8082`, `inventory-service:8083`, `shipping-service:8084`, `order-view-service:8086`, metrics_path `/actuator/prometheus`.
- [ ] **Step 4: Grafana provisioning** — datasource pointing at `http://prometheus:9090` (default); dashboard provider loading `/etc/grafana/provisioning/dashboards`; `order-saga.json` with 6 panels: HTTP p95 latency per service (`http_server_requests_seconds` histogram_quantile), request rate, consumer max lag (`kafka_consumer_fetch_manager_records_lag_max`), Kafka listener processing p95 (`spring_kafka_listener_seconds`), JVM heap used per service, DLT messages total (`ordersaga_dlt_messages_total`). Keep the JSON minimal but valid (schemaVersion ≥ 39, one datasource var).
- [ ] **Step 5: Compose** — prometheus (9090, mounts infra/prometheus.yml) + grafana (3000, `GF_AUTH_ANONYMOUS_ENABLED: "true"`, `GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer`, mounts provisioning dir), both `profiles: [obs]`, prometheus depends on nothing (scrape failures self-heal), grafana depends on prometheus started.
- [ ] **Step 6: Verify**

```bash
mvn -B verify
mvn -DskipTests package
docker compose --profile app --profile obs up --build -d
sleep 25
curl -s localhost:9090/api/v1/targets | jq '[.data.activeTargets[] | {job: .labels.instance, health: .health}]'   # all 5 up
curl -s localhost:8081/actuator/prometheus | head -5
curl -s -u admin:admin localhost:3000/api/dashboards/uid/order-saga 2>/dev/null | jq .dashboard.title 2>/dev/null || curl -s localhost:3000/api/search | jq .
docker compose --profile app --profile obs down
```

Expected: 5 healthy targets; dashboard present. Include outputs in report.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: prometheus metrics, DLT counter, provisioned grafana dashboard"
```

---

### Task 3: README + final verification + push

**Files:**
- Modify: `README.md`: new "### Observability" patterns subsection (trace-through-outbox mechanism — the traceparent column trick and why the outbox breaks traces otherwise; metrics + dashboard; how to run `--profile app --profile obs`; Jaeger UI 16686 / Grafana 3000 / Prometheus 9090); quickstart updated; roadmap Phase 4 struck accurately (claim only what shipped — screenshots pending is fine to say); note that screenshots are added separately.
- Create: `docs/img/.gitkeep` (screenshot slot — the controller captures Jaeger/Grafana screenshots after this task; README references `docs/img/jaeger-trace.png` and `docs/img/grafana-dashboard.png` with an HTML comment noting they're added post-capture, or simply describes the UIs without image tags if preferred — implementer's judgment, but NO broken image links may land in the README).

- [ ] **Step 1: README edits per above.**
- [ ] **Step 2: Full verification**

```bash
mvn -B verify
mvn -DskipTests package
docker compose --profile app up --build -d
mvn -pl e2e-tests test -De2e     # 3/3 — observability must not break the saga
docker compose --profile app down
```

- [ ] **Step 3: Commit + push**

```bash
git add README.md docs/img
git commit -m "docs: observability guide — tracing through the outbox, metrics, dashboards"
git push
```

Watch GitHub Actions stay green (workflow untouched).
