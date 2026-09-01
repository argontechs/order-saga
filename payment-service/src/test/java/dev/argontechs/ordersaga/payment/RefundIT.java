package dev.argontechs.ordersaga.payment;

import dev.argontechs.ordersaga.events.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class RefundIT extends AbstractKafkaIT {

    @Autowired KafkaTestSupport kafka;
    @Autowired PaymentRepository payments;

    @Test
    void outOfStockTriggersRefund() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.ORDERS, orderId, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(),
                "cust-1", List.of(new OrderItem("P100", 1, new BigDecimal("50.00"))), new BigDecimal("50.00")));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(payments.findByOrderId(orderId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.AUTHORIZED));

        kafka.send(Topics.INVENTORY, orderId,
                new OutOfStock(UUID.randomUUID(), orderId, Instant.now(), "P100"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(payments.findByOrderId(orderId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.REFUNDED));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(kafka.consume(Topics.PAYMENTS)).anySatisfy(r ->
                        assertThat(r.value()).isInstanceOf(PaymentRefunded.class)));
    }

    @Test
    void shipmentFailedTriggersRefund() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.ORDERS, orderId, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(),
                "cust-1", List.of(new OrderItem("P100", 1, new BigDecimal("50.00"))), new BigDecimal("50.00")));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(payments.findByOrderId(orderId)).isPresent());

        kafka.send(Topics.SHIPPING, orderId,
                new ShipmentFailed(UUID.randomUUID(), orderId, Instant.now(), "carrier unavailable"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(payments.findByOrderId(orderId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.REFUNDED));
    }
}
