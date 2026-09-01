# Phase 3: Kafka Streams Read Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** New `order-view-service` — a Kafka Streams application that materializes a per-order event timeline + current status from all four event topics and serves it via interactive queries at `GET /orders/{id}/timeline` (CQRS read side).

**Architecture:** One topology: four source streams (Avro `SpecificRecord` values via `SpecificAvroSerde`) merged → `groupByKey` (key = orderId string) → `aggregate` into an `OrderTimeline` POJO (JSON serde, view-local) held in a materialized state store. The aggregator dedups by eventId (at-least-once tolerance), appends `{type, occurredAt, detail}` entries, and derives status with the same forward-only rules as order-service's projection. REST layer reads the store through interactive queries. Unit tests use `TopologyTestDriver` (no broker — fast); one integration test uses Testcontainers Kafka + the `mock://` registry; e2e gains a timeline assertion.

**Tech Stack additions:** kafka-streams, `io.confluent:kafka-streams-avro-serde:7.7.0`, Spring for Apache Kafka `@EnableKafkaStreams`.

**Spec:** `docs/superpowers/specs/2026-09-01-order-saga-design.md` (Phase 3 row + "Read Side (CQRS, phase 3)" section; this plan implements only that).

## Global Constraints

- Everything from Phases 1-2 still binds: Java 21, Spring Boot 3.5.x, groupId `dev.argontechs`, base package `dev.argontechs.ordersaga`, commits by repo owner only with NO AI co-author trailer, conventional commits, main branch.
- **Port: 8086** (spec's Phase 1 plan once said 8085, but the registry host-maps 8085 — deviation ruled by controller, note it in README).
- Streams `application.id`: `order-view-service` (doubles as consumer group).
- No Postgres — state lives in Streams state stores (RocksDB + changelog topics), per spec.
- Registry URLs identical to sibling services: yml `http://localhost:8085`, compose env `http://schema-registry:8081`, tests `mock://ordersaga`.
- Statuses and transition rules MUST match order-service's projection: PENDING → PAID → RESERVED → CONFIRMED forward-only; PaymentFailed/OutOfStock/ShipmentFailed → CANCELLED with reason unless already CONFIRMED; terminal states immutable. OutOfStock reason format `out of stock: <productId>` (same as order-service).
- Timeline entries sorted by `occurredAt` when served.
- REST: `GET /orders/{id}/timeline` → 200 `{"orderId": "...", "status": "...", "events": [{"type": "OrderCreated", "occurredAt": "...", "detail": null|string}]}` or 404 when the store has no such key.

---

### Task 1: order-view-service module — topology + aggregator with TopologyTestDriver tests

**Files:**
- Create: `order-view-service/pom.xml` (add module to root pom)
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/OrderViewApplication.java`
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/OrderTimeline.java`
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/TimelineEntry.java`
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/TimelineAggregator.java`
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/TopologyConfig.java`
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/TopicConfig.java`
- Create: `order-view-service/src/main/resources/application.yml`
- Test: `order-view-service/src/test/java/dev/argontechs/ordersaga/view/TopologyTest.java`

**Interfaces:**
- Consumes: all Phase 2 event classes; Topics constants.
- Produces: state store named `order-timelines` holding `String orderId → OrderTimeline`; `OrderTimeline{orderId, status, eventIds:Set<String>, events:List<TimelineEntry>}`; `TimelineEntry{type, occurredAt, detail}`. Task 2's REST layer queries the store by name.

- [ ] **Step 1: Module pom**

`order-view-service/pom.xml` (usual parent block, then):

```xml
  <artifactId>order-view-service</artifactId>
  <dependencies>
    <dependency>
      <groupId>dev.argontechs</groupId>
      <artifactId>common-events</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
    </dependency>
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-streams-avro-serde</artifactId>
      <version>7.7.0</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams-test-utils</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-testcontainers</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>kafka</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.awaitility</groupId>
      <artifactId>awaitility</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
```

Add `<module>order-view-service</module>` to root pom. Note: NO data-jpa, NO flyway, NO postgres — this service has no database.

- [ ] **Step 2: Model + aggregator (write the failing TopologyTest first — Step 3 shows it; implement after seeing it fail to compile)**

`TimelineEntry.java`:

```java
package dev.argontechs.ordersaga.view;

import java.time.Instant;

public record TimelineEntry(String type, Instant occurredAt, String detail) {}
```

`OrderTimeline.java`:

```java
package dev.argontechs.ordersaga.view;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Mutable aggregate held in the Streams state store (JSON-serialized). */
public class OrderTimeline {
    public String orderId;
    public String status = "PENDING";
    public Set<String> eventIds = new HashSet<>();
    public List<TimelineEntry> events = new ArrayList<>();
}
```

(Public fields keep the JSON serde trivial; this is a view-local storage shape, not an API contract — the REST layer decides the response shape.)

`TimelineAggregator.java`:

```java
package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.*;
import org.apache.avro.specific.SpecificRecordBase;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Folds saga events into the per-order timeline. Idempotent by eventId
 *  (Streams is at-least-once; a reprocessed record must not duplicate entries). */
public class TimelineAggregator {

    private static final List<String> FORWARD = List.of("PENDING", "PAID", "RESERVED", "CONFIRMED");

    public OrderTimeline apply(String orderIdKey, SpecificRecordBase event, OrderTimeline timeline) {
        var eventId = ((UUID) event.get("eventId")).toString();
        if (!timeline.eventIds.add(eventId)) return timeline;

        timeline.orderId = orderIdKey;
        var occurredAt = (Instant) event.get("occurredAt");
        String detail = null;

        switch (event) {
            case OrderCreated e -> advance(timeline, "PENDING");
            case PaymentAuthorized e -> advance(timeline, "PAID");
            case InventoryReserved e -> advance(timeline, "RESERVED");
            case OrderShipped e -> advance(timeline, "CONFIRMED");
            case PaymentFailed e -> { detail = e.getReason(); cancel(timeline, detail); }
            case OutOfStock e -> { detail = "out of stock: " + e.getProductId(); cancel(timeline, detail); }
            case ShipmentFailed e -> { detail = e.getReason(); cancel(timeline, detail); }
            case PaymentRefunded e -> detail = "refund issued";
            case InventoryReleased e -> detail = "stock released";
            default -> { }
        }
        timeline.events.add(new TimelineEntry(event.getClass().getSimpleName(), occurredAt, detail));
        return timeline;
    }

    private void advance(OrderTimeline t, String next) {
        if (t.status.equals("CANCELLED") || t.status.equals("CONFIRMED")) return;
        if (FORWARD.indexOf(next) > FORWARD.indexOf(t.status)) t.status = next;
        // OrderCreated on a fresh timeline: indexOf("PENDING") == indexOf("PENDING"), stays PENDING — fine.
    }

    private void cancel(OrderTimeline t, String reason) {
        if (t.status.equals("CONFIRMED") || t.status.equals("CANCELLED")) return;
        t.status = "CANCELLED";
    }
}
```

`TopologyConfig.java`:

```java
package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.Topics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

@Configuration
@EnableKafkaStreams
public class TopologyConfig {

    public static final String STORE = "order-timelines";

    @Bean
    public KStream<String, SpecificRecordBase> timelineTopology(StreamsBuilder builder,
            @Value("${spring.kafka.properties.schema.registry.url}") String registryUrl) {

        var valueSerde = new SpecificAvroSerde<SpecificRecordBase>();
        valueSerde.configure(Map.of(
                "schema.registry.url", registryUrl,
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                "specific.avro.reader", "true"), false);

        var consumed = Consumed.with(Serdes.String(), valueSerde);
        var stream = builder.<String, SpecificRecordBase>stream(Topics.ORDERS, consumed)
                .merge(builder.stream(Topics.PAYMENTS, consumed))
                .merge(builder.stream(Topics.INVENTORY, consumed))
                .merge(builder.stream(Topics.SHIPPING, consumed));

        var aggregator = new TimelineAggregator();
        stream.groupByKey()
              .aggregate(OrderTimeline::new,
                         (key, event, timeline) -> aggregator.apply(key, event, timeline),
                         Materialized.<String, OrderTimeline, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE)
                                 .withKeySerde(Serdes.String())
                                 .withValueSerde(new JsonSerde<>(OrderTimeline.class)));
        return stream;
    }
}
```

`OrderViewApplication.java` — plain `@SpringBootApplication` (package `dev.argontechs.ordersaga.view`; NO ordersaga-wide component scan — this service must not pick up common-messaging's JPA/outbox beans; it has no DataSource).

`TopicConfig.java` — broker auto-create is OFF and Kafka Streams fails fast on missing source topics; in the composed stack this service can boot before its siblings have created them. Declaring the same four `NewTopic` beans here is idempotent (created-if-missing, same 3 partitions/1 replica as the owners) and removes the startup race. Spring's `KafkaAdmin` creates topics during singleton initialization, before `StreamsBuilderFactoryBean`'s `SmartLifecycle` start:

```java
package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Not the owner of these topics — declared defensively (idempotent create-if-missing)
 *  because Streams fails fast on missing source topics and auto-create is off. */
@Configuration
public class TopicConfig {
    @Bean NewTopic ordersTopic()    { return TopicBuilder.name(Topics.ORDERS).partitions(3).replicas(1).build(); }
    @Bean NewTopic paymentsTopic()  { return TopicBuilder.name(Topics.PAYMENTS).partitions(3).replicas(1).build(); }
    @Bean NewTopic inventoryTopic() { return TopicBuilder.name(Topics.INVENTORY).partitions(3).replicas(1).build(); }
    @Bean NewTopic shippingTopic()  { return TopicBuilder.name(Topics.SHIPPING).partitions(3).replicas(1).build(); }
}
```

`application.yml`:

```yaml
server:
  port: 8086
spring:
  application:
    name: order-view-service
  kafka:
    bootstrap-servers: localhost:9092
    properties:
      schema.registry.url: http://localhost:8085
    streams:
      application-id: order-view-service
      properties:
        processing.guarantee: at_least_once
        commit.interval.ms: 1000
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
```

- [ ] **Step 3: TopologyTestDriver tests (write FIRST, watch fail, then implement Step 2)**

`TopologyTest.java`:

```java
package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyTest {

    TopologyTestDriver driver;
    TestInputTopic<String, SpecificRecordBase> orders, payments, inventory, shipping;
    KeyValueStore<String, OrderTimeline> store;

    @BeforeEach
    void setUp() {
        var builder = new StreamsBuilder();
        new TopologyConfig().timelineTopology(builder, "mock://topology-test");
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);

        var serde = new SpecificAvroSerde<SpecificRecordBase>();
        serde.configure(Map.of(
                "schema.registry.url", "mock://topology-test",
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                "specific.avro.reader", "true"), false);

        orders = driver.createInputTopic(Topics.ORDERS, new StringSerializer(), serde.serializer());
        payments = driver.createInputTopic(Topics.PAYMENTS, new StringSerializer(), serde.serializer());
        inventory = driver.createInputTopic(Topics.INVENTORY, new StringSerializer(), serde.serializer());
        shipping = driver.createInputTopic(Topics.SHIPPING, new StringSerializer(), serde.serializer());
        store = driver.getKeyValueStore(TopologyConfig.STORE);
    }

    @AfterEach
    void tearDown() { driver.close(); }

    private UUID happyOrder() {
        var orderId = UUID.randomUUID();
        var key = orderId.toString();
        var items = List.of(new OrderItem("P100", 1, new BigDecimal("10.00")));
        orders.pipeInput(key, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "c1", items, new BigDecimal("10.00")));
        payments.pipeInput(key, new PaymentAuthorized(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID(), new BigDecimal("10.00"), items));
        inventory.pipeInput(key, new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));
        shipping.pipeInput(key, new OrderShipped(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID()));
        return orderId;
    }

    @Test
    void happyPathBuildsConfirmedTimeline() {
        var orderId = happyOrder();
        var timeline = store.get(orderId.toString());
        assertThat(timeline.status).isEqualTo("CONFIRMED");
        assertThat(timeline.events).extracting(TimelineEntry::type)
                .containsExactly("OrderCreated", "PaymentAuthorized", "InventoryReserved", "OrderShipped");
    }

    @Test
    void outOfStockCancelsAndRecordsCompensation() {
        var orderId = UUID.randomUUID();
        var key = orderId.toString();
        var items = List.of(new OrderItem("P200", 50, new BigDecimal("10.00")));
        orders.pipeInput(key, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "c1", items, new BigDecimal("500.00")));
        payments.pipeInput(key, new PaymentAuthorized(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID(), new BigDecimal("500.00"), items));
        inventory.pipeInput(key, new OutOfStock(UUID.randomUUID(), orderId, Instant.now(), "P200"));
        payments.pipeInput(key, new PaymentRefunded(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID()));

        var timeline = store.get(key);
        assertThat(timeline.status).isEqualTo("CANCELLED");
        assertThat(timeline.events).extracting(TimelineEntry::type)
                .contains("OutOfStock", "PaymentRefunded");
        assertThat(timeline.events.stream().filter(e -> e.type().equals("OutOfStock")).findFirst().orElseThrow().detail())
                .isEqualTo("out of stock: P200");
    }

    @Test
    void duplicateEventIsIgnored() {
        var orderId = UUID.randomUUID();
        var key = orderId.toString();
        var created = new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "c1",
                List.of(new OrderItem("P100", 1, new BigDecimal("10.00"))), new BigDecimal("10.00"));
        orders.pipeInput(key, created);
        orders.pipeInput(key, created);
        assertThat(store.get(key).events).hasSize(1);
    }

    @Test
    void lateFailureAfterConfirmedDoesNotChangeStatus() {
        var orderId = happyOrder();
        shipping.pipeInput(orderId.toString(),
                new ShipmentFailed(UUID.randomUUID(), orderId, Instant.now(), "late"));
        var timeline = store.get(orderId.toString());
        assertThat(timeline.status).isEqualTo("CONFIRMED");
        assertThat(timeline.events).extracting(TimelineEntry::type).contains("ShipmentFailed"); // recorded, but status unchanged
    }
}
```

- [ ] **Step 4: Run to fail, implement, run to pass**

Run: `mvn -pl order-view-service test` — first compilation failure, then after implementing Step 2: PASS (4 tests). Note: `TopologyTestDriver` + `mock://` registry needs no broker or Docker.

- [ ] **Step 5: Commit**

```bash
git add pom.xml order-view-service
git commit -m "feat(view): kafka streams timeline topology with idempotent aggregation"
```

---

### Task 2: Interactive query REST endpoint + integration test

**Files:**
- Create: `order-view-service/src/main/java/dev/argontechs/ordersaga/view/TimelineController.java`
- Test: `order-view-service/src/test/java/dev/argontechs/ordersaga/view/TimelineQueryIT.java`

**Interfaces:**
- Consumes: Task 1's store.
- Produces: `GET /orders/{id}/timeline` per Global Constraints (events sorted by occurredAt; 404 on unknown id; 503 while Streams is not yet RUNNING).

- [ ] **Step 1: Controller**

`TimelineController.java`:

```java
package dev.argontechs.ordersaga.view;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class TimelineController {

    private final StreamsBuilderFactoryBean factory;

    public TimelineController(StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }

    @GetMapping("/orders/{id}/timeline")
    public ResponseEntity<Map<String, Object>> timeline(@PathVariable UUID id) {
        var streams = factory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        var store = streams.store(StoreQueryParameters.fromNameAndType(
                TopologyConfig.STORE, QueryableStoreTypes.<String, OrderTimeline>keyValueStore()));
        var timeline = store.get(id.toString());
        if (timeline == null) return ResponseEntity.notFound().build();

        var events = timeline.events.stream()
                .sorted(Comparator.comparing(TimelineEntry::occurredAt))
                .map(e -> {
                    var entry = new java.util.LinkedHashMap<String, Object>();
                    entry.put("type", e.type());
                    entry.put("occurredAt", e.occurredAt().toString());
                    entry.put("detail", e.detail());
                    return entry;
                })
                .toList();
        return ResponseEntity.ok(Map.of(
                "orderId", timeline.orderId,
                "status", timeline.status,
                "events", events));
    }
}
```

- [ ] **Step 2: Integration test (real Kafka via Testcontainers, mock:// registry, real Streams runtime)**

`TimelineQueryIT.java`:

```java
package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TimelineQueryIT.Containers.class)
class TimelineQueryIT {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");
        static { kafka.start(); }

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() { return kafka; }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", Containers.kafka::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", () -> "mock://ordersaga");
        registry.add("spring.kafka.streams.properties.state.dir",
                () -> System.getProperty("java.io.tmpdir") + "/kstreams-test-" + UUID.randomUUID());
    }

    @Autowired TestRestTemplate rest;

    private void send(String topic, UUID orderId, Object event) {
        var serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(
                "bootstrap.servers", Containers.kafka.getBootstrapServers(),
                "schema.registry.url", "mock://ordersaga",
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy"), false);
        try (var producer = new KafkaProducer<String, Object>(
                Map.of("bootstrap.servers", Containers.kafka.getBootstrapServers()),
                new StringSerializer(), serializer)) {
            producer.send(new ProducerRecord<>(topic, orderId.toString(), event)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void servesTimelineForOrderAndNotFoundForUnknown() {
        var orderId = UUID.randomUUID();
        var items = List.of(new OrderItem("P100", 1, new BigDecimal("10.00")));
        send(Topics.ORDERS, orderId, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "c1", items, new BigDecimal("10.00")));
        send(Topics.PAYMENTS, orderId, new PaymentAuthorized(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID(), new BigDecimal("10.00"), items));

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            var response = rest.getForEntity("/orders/" + orderId + "/timeline", Map.class);
            assertThat(response.getStatusCode().value()).isEqualTo(200);
            assertThat(response.getBody().get("status")).isEqualTo("PAID");
            assertThat((List<Map<String, Object>>) response.getBody().get("events")).hasSize(2);
        });

        assertThat(rest.getForEntity("/orders/" + UUID.randomUUID() + "/timeline", Map.class)
                .getStatusCode().value()).isEqualTo(404);
    }
}
```

(Note: source topics are auto-created by the Testcontainers broker on Streams subscribe — default auto-create is on in the test broker, unlike compose; that's fine here.)

- [ ] **Step 3: Run, fix, pass**

Run: `mvn -pl order-view-service test` — all tests (topology 4 + IT 1) green. Then full `mvn -B verify`.

- [ ] **Step 4: Commit**

```bash
git add order-view-service
git commit -m "feat(view): timeline endpoint via interactive queries"
```

---

### Task 3: Compose + Dockerfile + e2e timeline assertion + docs

**Files:**
- Create: `order-view-service/Dockerfile` (identical to siblings)
- Modify: `docker-compose.yml` (order-view-service under `app` profile: port 8086, `SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092`, `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL: http://schema-registry:8081`, depends_on kafka + schema-registry healthy; NO postgres dependency)
- Modify: `e2e-tests/src/test/java/dev/argontechs/ordersaga/e2e/OrderSagaE2ETest.java` (after the happy-path order reaches CONFIRMED, poll `http://localhost:8086/orders/{id}/timeline` until 200 with status CONFIRMED and ≥4 events; assert types contain OrderCreated/PaymentAuthorized/InventoryReserved/OrderShipped)
- Modify: `README.md` (architecture diagram gains order-view-service; new "CQRS read model (Kafka Streams)" pattern paragraph: merged topics → idempotent aggregate → state store → interactive queries; port table/quickstart mention 8086 + why not 8085; roadmap Phase 3 → done)
- Verify: CI unchanged

- [ ] **Step 1: Dockerfile + compose block**
- [ ] **Step 2: Full verification**

```bash
mvn -B verify
mvn -DskipTests package
docker compose --profile app up --build -d
mvn -pl e2e-tests test -De2e     # now includes the timeline assertion
docker compose --profile app down
```

- [ ] **Step 3: README updates**
- [ ] **Step 4: Commit + push**

```bash
git add order-view-service/Dockerfile docker-compose.yml e2e-tests README.md
git commit -m "feat: order-view-service in compose, e2e timeline check, document CQRS read model"
git push
```

Watch GitHub Actions stay green.
