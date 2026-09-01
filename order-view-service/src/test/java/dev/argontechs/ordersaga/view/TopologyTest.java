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
