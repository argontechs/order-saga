package dev.argontechs.ordersaga.inventory;

import dev.argontechs.ordersaga.events.*;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class ReleaseIT extends AbstractKafkaIT {

    @Autowired KafkaTestSupport kafka;
    @Autowired EntityManager em;
    @Autowired TransactionTemplate tx;

    private int available(String productId) {
        return tx.execute(s -> ((Number) em.createNativeQuery(
                "SELECT available FROM stock WHERE product_id = ?1")
                .setParameter(1, productId).getSingleResult()).intValue());
    }

    @Test
    void shipmentFailedRestoresReservedStock() {
        // Baseline captured live rather than hardcoded: this module's IT classes share one
        // Testcontainers Postgres/Kafka context (see AbstractKafkaIT), so other classes (e.g.
        // InventoryIT) may already have mutated P100's stock by the time this test runs.
        var baseline = available("P100");
        var orderId = UUID.randomUUID();
        kafka.send(Topics.PAYMENTS, orderId, new PaymentAuthorized(UUID.randomUUID(), orderId,
                Instant.now(), UUID.randomUUID(), new BigDecimal("40.00"),
                List.of(new OrderItem("P100", 4, new BigDecimal("10.00")))));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(available("P100")).isEqualTo(baseline - 4));

        kafka.send(Topics.SHIPPING, orderId,
                new ShipmentFailed(UUID.randomUUID(), orderId, Instant.now(), "carrier unavailable"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(available("P100")).isEqualTo(baseline));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(kafka.consume(Topics.INVENTORY)).anySatisfy(r ->
                        assertThat(r.value()).isInstanceOf(InventoryReleased.class)));
    }
}
