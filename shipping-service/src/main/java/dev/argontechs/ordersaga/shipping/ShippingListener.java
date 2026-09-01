package dev.argontechs.ordersaga.shipping;

import dev.argontechs.ordersaga.events.*;
import dev.argontechs.ordersaga.messaging.IdempotencyGuard;
import dev.argontechs.ordersaga.messaging.OutboxWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@KafkaListener(topics = Topics.INVENTORY, groupId = "${spring.kafka.consumer.group-id}")
public class ShippingListener {

    private final ShipmentRepository shipments;
    private final OutboxWriter outbox;
    private final IdempotencyGuard guard;
    private final double failureRate;

    public ShippingListener(ShipmentRepository shipments, OutboxWriter outbox, IdempotencyGuard guard,
                            @Value("${shipping.failure-rate:0.0}") double failureRate) {
        this.shipments = shipments;
        this.outbox = outbox;
        this.guard = guard;
        this.failureRate = failureRate;
    }

    @KafkaHandler
    @Transactional
    public void on(InventoryReserved event) {
        if (!guard.firstTime(event.getEventId())) return;

        var shipmentId = UUID.randomUUID();
        if (ThreadLocalRandom.current().nextDouble() >= failureRate) {
            shipments.save(new Shipment(shipmentId, event.getOrderId(), ShipmentStatus.CREATED));
            outbox.write(Topics.SHIPPING, event.getOrderId(),
                    new OrderShipped(UUID.randomUUID(), event.getOrderId(), Instant.now(), shipmentId));
        } else {
            shipments.save(new Shipment(shipmentId, event.getOrderId(), ShipmentStatus.FAILED));
            outbox.write(Topics.SHIPPING, event.getOrderId(),
                    new ShipmentFailed(UUID.randomUUID(), event.getOrderId(), Instant.now(), "carrier unavailable"));
        }
    }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) {}
}
