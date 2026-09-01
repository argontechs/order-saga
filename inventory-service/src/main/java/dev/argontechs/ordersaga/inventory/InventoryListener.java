package dev.argontechs.ordersaga.inventory;

import dev.argontechs.ordersaga.events.PaymentAuthorized;
import dev.argontechs.ordersaga.events.ShipmentFailed;
import dev.argontechs.ordersaga.events.Topics;
import dev.argontechs.ordersaga.messaging.IdempotencyGuard;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@KafkaListener(topics = {Topics.PAYMENTS, Topics.SHIPPING}, groupId = "${spring.kafka.consumer.group-id}")
public class InventoryListener {

    private final InventoryService inventory;
    private final IdempotencyGuard guard;

    public InventoryListener(InventoryService inventory, IdempotencyGuard guard) {
        this.inventory = inventory;
        this.guard = guard;
    }

    @KafkaHandler
    @Transactional
    public void on(PaymentAuthorized event) {
        if (!guard.firstTime(event.getEventId())) return;
        inventory.reserve(event.getOrderId(), event.getItems());
    }

    @KafkaHandler
    @Transactional
    public void on(ShipmentFailed event) {
        if (!guard.firstTime(event.getEventId())) return;
        inventory.release(event.getOrderId());
    }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) {}
}
