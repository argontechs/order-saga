package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class OutboxPublishIT extends AbstractKafkaIT {

    @Autowired TestRestTemplate rest;
    @Autowired org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;

    @Test
    void orderCreatedReachesKafkaWithOrderIdKeyAndTypeHeader() throws InterruptedException {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 1, "unitPrice", 10.00)));
        var orderId = (String) rest.postForEntity("/orders", body, Map.class).getBody().get("orderId");

        // Wait for scheduler to publish the event
        Thread.sleep(1500);

        String bootstrapServers = String.join(",", kafkaProperties.getBootstrapServers());
        var consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test-" + UUID.randomUUID(), "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
                consumerProps,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new org.apache.kafka.common.serialization.StringDeserializer())) {
            consumer.subscribe(java.util.List.of("orders.events"));
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
            var record = java.util.stream.StreamSupport.stream(records.records("orders.events").spliterator(), false)
                    .filter(r -> orderId.equals(r.key()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No record found for order " + orderId));
            assertThat(record.key()).isEqualTo(orderId);
            assertThat(record.value()).contains(orderId);
            assertThat(new String(record.headers().lastHeader("__TypeId__").value()))
                    .isEqualTo(OrderCreated.class.getName());
        }
    }
}
