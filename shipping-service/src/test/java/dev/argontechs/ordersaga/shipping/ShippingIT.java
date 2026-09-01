package dev.argontechs.ordersaga.shipping;

import dev.argontechs.ordersaga.events.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ShippingIT {

    @SpringBootTest
    @Import({TestcontainersConfig.class, KafkaTestSupport.class})
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
    static class HappyPath extends AbstractKafkaIT {
        @Autowired KafkaTestSupport kafka;
        @Autowired ShipmentRepository shipments;

        @Test
        void createsShipmentAndEmitsOrderShipped() {
            var orderId = UUID.randomUUID();
            kafka.send(Topics.INVENTORY, orderId,
                    new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(shipments.findByOrderId(orderId)).isPresent());
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(kafka.consume(Topics.SHIPPING)).anySatisfy(r ->
                            assertThat(r.value()).isInstanceOf(OrderShipped.class)));
        }
    }

    @SpringBootTest
    @Import({TestcontainersConfig.class, KafkaTestSupport.class})
    @TestPropertySource(properties = "shipping.failure-rate=1.0")
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
    static class ForcedFailure extends AbstractKafkaIT {
        @Autowired KafkaTestSupport kafka;

        @Test
        void emitsShipmentFailedWhenCarrierFails() {
            var orderId = UUID.randomUUID();
            kafka.send(Topics.INVENTORY, orderId,
                    new InventoryReserved(UUID.randomUUID(), orderId, Instant.now()));

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(kafka.consume(Topics.SHIPPING)).anySatisfy(r ->
                            assertThat(r.value()).isInstanceOf(ShipmentFailed.class)));
        }
    }
}
