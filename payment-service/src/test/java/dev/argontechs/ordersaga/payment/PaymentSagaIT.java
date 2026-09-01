package dev.argontechs.ordersaga.payment;

import dev.argontechs.ordersaga.events.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class PaymentSagaIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", TestcontainersConfig::getBootstrapServers);
    }

    @Autowired KafkaTestSupport kafka;
    @Autowired PaymentRepository payments;

    private OrderCreated orderCreated(UUID orderId, String amount) {
        return new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "cust-1",
                List.of(new OrderItem("P100", 1, new BigDecimal(amount))), new BigDecimal(amount));
    }

    @Test
    void authorizesNormalOrderAndEmitsPaymentAuthorized() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.ORDERS, orderId, orderCreated(orderId, "99.80"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var payment = payments.findByOrderId(orderId).orElseThrow();
            assertThat(payment.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
        });
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var records = kafka.consume(Topics.PAYMENTS);
            assertThat(records).anySatisfy(r -> {
                assertThat(r.key()).isEqualTo(orderId.toString());
                assertThat(new String(r.headers().lastHeader("__TypeId__").value()))
                        .isEqualTo(PaymentAuthorized.class.getName());
            });
        });
    }

    @Test
    void declinesExpensiveOrderAndEmitsPaymentFailed() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.ORDERS, orderId, orderCreated(orderId, "15000.00"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var payment = payments.findByOrderId(orderId).orElseThrow();
            assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        });
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(kafka.consume(Topics.PAYMENTS)).anySatisfy(r ->
                assertThat(new String(r.headers().lastHeader("__TypeId__").value()))
                        .isEqualTo(PaymentFailed.class.getName())));
    }

    @Test
    void duplicateDeliveryIsNoOp() {
        var orderId = UUID.randomUUID();
        var event = orderCreated(orderId, "50.00"); // same eventId both sends
        kafka.send(Topics.ORDERS, orderId, event);
        kafka.send(Topics.ORDERS, orderId, event);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(payments.findByOrderId(orderId)).isPresent());
        // second delivery must not create another payment or blow up on the UNIQUE(order_id)
        await().during(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(payments.findAll().stream().filter(p -> p.getOrderId().equals(orderId)).count())
                        .isEqualTo(1));
    }
}
