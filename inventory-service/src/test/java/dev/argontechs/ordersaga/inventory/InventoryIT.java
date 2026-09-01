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
class InventoryIT extends AbstractKafkaIT {

    @Autowired KafkaTestSupport kafka;
    @Autowired EntityManager em;
    @Autowired TransactionTemplate tx;

    private PaymentAuthorized paid(UUID orderId, String productId, int qty) {
        return new PaymentAuthorized(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID(),
                new BigDecimal("100.00"), List.of(new OrderItem(productId, qty, new BigDecimal("10.00"))));
    }

    private int available(String productId) {
        return tx.execute(s -> ((Number) em.createNativeQuery(
                "SELECT available FROM stock WHERE product_id = ?1")
                .setParameter(1, productId).getSingleResult()).intValue());
    }

    @Test
    void reservesStockAndEmitsInventoryReserved() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.PAYMENTS, orderId, paid(orderId, "P100", 3));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(available("P100")).isEqualTo(97));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(kafka.consume(Topics.INVENTORY)).anySatisfy(r -> {
                    assertThat(r.key()).isEqualTo(orderId.toString());
                    assertThat(r.value()).isInstanceOf(InventoryReserved.class);
                }));
    }

    @Test
    void insufficientStockEmitsOutOfStockAndChangesNothing() {
        var orderId = UUID.randomUUID();
        kafka.send(Topics.PAYMENTS, orderId, paid(orderId, "P200", 50)); // only 5 seeded

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(kafka.consume(Topics.INVENTORY)).anySatisfy(r -> {
                    assertThat(r.key()).isEqualTo(orderId.toString());
                    assertThat(r.value()).isInstanceOf(OutOfStock.class);
                }));
        assertThat(available("P200")).isEqualTo(5);
    }
}
