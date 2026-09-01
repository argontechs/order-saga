package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class OutboxPublishIT extends AbstractKafkaIT {

    @Autowired TestRestTemplate rest;
    @Autowired KafkaTestSupport kafka;

    @Test
    void orderCreatedReachesKafkaWithOrderIdKeyAndAvroType() {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 1, "unitPrice", 10.00)));
        var orderId = (String) rest.postForEntity("/orders", body, Map.class).getBody().get("orderId");

        // Poll (fresh consumer group each attempt, from earliest) until the scheduler has
        // published this order's event — the topic may already hold unrelated records from
        // other tests, so a single poll isn't enough to guarantee this order's record arrived.
        var record = await().atMost(Duration.ofSeconds(15)).until(() -> findRecord(orderId), Optional::isPresent).get();

        assertThat(record.key()).isEqualTo(orderId);
        assertThat(record.value()).isInstanceOf(OrderCreated.class);
        assertThat(((OrderCreated) record.value()).getOrderId().toString()).isEqualTo(orderId);
    }

    private Optional<ConsumerRecord<String, Object>> findRecord(String orderId) {
        var records = kafka.consume("orders.events");
        return StreamSupport.stream(records.records("orders.events").spliterator(), false)
                .filter(r -> orderId.equals(r.key()))
                .findFirst();
    }
}
