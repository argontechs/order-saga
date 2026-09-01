package dev.argontechs.ordersaga.payment;

import dev.argontechs.ordersaga.events.*;
import dev.argontechs.ordersaga.messaging.IdempotencyGuard;
import dev.argontechs.ordersaga.messaging.OutboxWriter;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
@KafkaListener(topics = {Topics.ORDERS, Topics.INVENTORY, Topics.SHIPPING},
               groupId = "${spring.kafka.consumer.group-id}")
public class PaymentListener {

    private final PaymentRepository payments;
    private final FakePsp psp;
    private final OutboxWriter outbox;
    private final IdempotencyGuard guard;

    public PaymentListener(PaymentRepository payments, FakePsp psp,
                           OutboxWriter outbox, IdempotencyGuard guard) {
        this.payments = payments;
        this.psp = psp;
        this.outbox = outbox;
        this.guard = guard;
    }

    @KafkaHandler
    @Transactional
    public void on(OrderCreated event) {
        if (!guard.firstTime(event.getEventId())) return;

        var paymentId = UUID.randomUUID();
        if (psp.authorize(event.getTotalAmount())) {
            payments.save(new Payment(paymentId, event.getOrderId(), event.getTotalAmount(), PaymentStatus.AUTHORIZED));
            outbox.write(Topics.PAYMENTS, event.getOrderId(), new PaymentAuthorized(
                    UUID.randomUUID(), event.getOrderId(), Instant.now(), paymentId,
                    event.getTotalAmount(), event.getItems()));
        } else {
            payments.save(new Payment(paymentId, event.getOrderId(), event.getTotalAmount(), PaymentStatus.FAILED));
            outbox.write(Topics.PAYMENTS, event.getOrderId(), new PaymentFailed(
                    UUID.randomUUID(), event.getOrderId(), Instant.now(), "declined by PSP"));
        }
    }

    @KafkaHandler
    @Transactional
    public void on(OutOfStock event) {
        refund(event.getEventId(), event.getOrderId());
    }

    @KafkaHandler
    @Transactional
    public void on(ShipmentFailed event) {
        refund(event.getEventId(), event.getOrderId());
    }

    private void refund(UUID eventId, UUID orderId) {
        if (!guard.firstTime(eventId)) return;
        payments.findByOrderId(orderId).ifPresent(payment -> {
            if (payment.refund()) {
                payments.save(payment);
                outbox.write(Topics.PAYMENTS, orderId, new PaymentRefunded(
                        UUID.randomUUID(), orderId, Instant.now(), payment.getId()));
            }
        });
    }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) { /* other event types on subscribed topics: not ours */ }
}
