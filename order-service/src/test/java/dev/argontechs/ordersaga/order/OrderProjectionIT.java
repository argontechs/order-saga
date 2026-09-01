package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class OrderProjectionIT extends AbstractKafkaIT {

    @Autowired TestRestTemplate rest;
    @Autowired KafkaTestSupport kafka;

    private UUID createOrder() {
        var body = Map.of("customerId", "cust-1",
                "items", List.of(Map.of("productId", "P100", "quantity", 1, "unitPrice", 10.00)));
        return UUID.fromString((String) rest.postForEntity("/orders", body, Map.class).getBody().get("orderId"));
    }

    private String status(UUID orderId) {
        return (String) rest.getForEntity("/orders/" + orderId, Map.class).getBody().get("status");
    }

    @Test
    void progressEventsAdvanceStatusToConfirmed() {
        var orderId = createOrder();
        kafka.send(Topics.PAYMENTS, orderId, new PaymentAuthorized(UUID.randomUUID(), orderId,
                Instant.now(), UUID.randomUUID(), BigDecimal.TEN,
                List.of(new OrderItem("P100", 1, BigDecimal.TEN))));
        kafka.send(Topics.INVENTORY, orderId, new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));
        kafka.send(Topics.SHIPPING, orderId, new OrderShipped(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID()));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(status(orderId)).isEqualTo("CONFIRMED"));
    }

    @Test
    void terminalFailureCancelsOrder() {
        var orderId = createOrder();
        kafka.send(Topics.PAYMENTS, orderId, new PaymentFailed(UUID.randomUUID(), orderId,
                Instant.now(), "declined by PSP"));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(status(orderId)).isEqualTo("CANCELLED"));
        assertThat((String) rest.getForEntity("/orders/" + orderId, Map.class)
                .getBody().get("cancellationReason")).isEqualTo("declined by PSP");
    }

    @Test
    void confirmedOrderIgnoresLateFailureEvents() {
        var orderId = createOrder();
        kafka.send(Topics.SHIPPING, orderId, new OrderShipped(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID()));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(status(orderId)).isEqualTo("CONFIRMED"));

        kafka.send(Topics.SHIPPING, orderId, new ShipmentFailed(UUID.randomUUID(), orderId, Instant.now(), "late"));
        await().during(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(status(orderId)).isEqualTo("CONFIRMED"));
    }
}
