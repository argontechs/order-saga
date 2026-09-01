package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class OutboxPublishIT extends AbstractKafkaIT {

    @Autowired TestRestTemplate rest;
    @Autowired org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;

    @Test
    void orderCreatedReachesKafkaWithOrderIdKeyAndTypeHeader() {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 1, "unitPrice", 10.00)));
        var orderId = (String) rest.postForEntity("/orders", body, Map.class).getBody().get("orderId");

        // Poll (fresh consumer group each attempt, from earliest) until the scheduler has
        // published this order's event — the topic may already hold unrelated records from
        // other tests, so a single poll isn't enough to guarantee this order's record arrived.
        var record = await().atMost(Duration.ofSeconds(15)).until(() -> findRecord(orderId), Optional::isPresent).get();

        assertThat(record.key()).isEqualTo(orderId);
        assertThat(record.value()).contains(orderId);
        assertThat(new String(record.headers().lastHeader("__TypeId__").value()))
                .isEqualTo(OrderCreated.class.getName());
    }

    private Optional<ConsumerRecord<String, String>> findRecord(String orderId) {
        String bootstrapServers = String.join(",", kafkaProperties.getBootstrapServers());
        var consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test-" + UUID.randomUUID(), "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
                consumerProps,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new org.apache.kafka.common.serialization.StringDeserializer())) {
            consumer.subscribe(java.util.List.of("orders.events"));
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
            return java.util.stream.StreamSupport.stream(records.records("orders.events").spliterator(), false)
                    .filter(r -> orderId.equals(r.key()))
                    .findFirst();
        }
    }
}
