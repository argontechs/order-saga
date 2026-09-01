package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.*;
import dev.argontechs.ordersaga.messaging.IdempotencyGuard;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.function.Consumer;

@Component
@KafkaListener(topics = {Topics.PAYMENTS, Topics.INVENTORY, Topics.SHIPPING},
               groupId = "${spring.kafka.consumer.group-id}")
public class OrderProjectionListener {

    private final OrderRepository orders;
    private final IdempotencyGuard guard;

    public OrderProjectionListener(OrderRepository orders, IdempotencyGuard guard) {
        this.orders = orders;
        this.guard = guard;
    }

    @KafkaHandler @Transactional
    public void on(PaymentAuthorized e) { apply(e.getEventId(), e.getOrderId(), o -> o.advanceTo(OrderStatus.PAID)); }

    @KafkaHandler @Transactional
    public void on(InventoryReserved e) { apply(e.getEventId(), e.getOrderId(), o -> o.advanceTo(OrderStatus.RESERVED)); }

    @KafkaHandler @Transactional
    public void on(OrderShipped e) { apply(e.getEventId(), e.getOrderId(), o -> o.advanceTo(OrderStatus.CONFIRMED)); }

    @KafkaHandler @Transactional
    public void on(PaymentFailed e) { apply(e.getEventId(), e.getOrderId(), o -> o.cancel(e.getReason())); }

    @KafkaHandler @Transactional
    public void on(OutOfStock e) { apply(e.getEventId(), e.getOrderId(), o -> o.cancel("out of stock: " + e.getProductId())); }

    @KafkaHandler @Transactional
    public void on(ShipmentFailed e) { apply(e.getEventId(), e.getOrderId(), o -> o.cancel(e.getReason())); }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) {}

    private void apply(UUID eventId, UUID orderId, Consumer<Order> change) {
        if (!guard.firstTime(eventId)) return;
        orders.findById(orderId).ifPresent(order -> {
            change.accept(order);
            orders.save(order);
        });
    }
}
