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
@KafkaListener(topics = Topics.ORDERS, groupId = "${spring.kafka.consumer.group-id}")
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
        if (!guard.firstTime(event.eventId())) return;

        var paymentId = UUID.randomUUID();
        if (psp.authorize(event.totalAmount())) {
            payments.save(new Payment(paymentId, event.orderId(), event.totalAmount(), PaymentStatus.AUTHORIZED));
            outbox.write(Topics.PAYMENTS, event.orderId(), new PaymentAuthorized(
                    UUID.randomUUID(), event.orderId(), Instant.now(), paymentId,
                    event.totalAmount(), event.items()));
        } else {
            payments.save(new Payment(paymentId, event.orderId(), event.totalAmount(), PaymentStatus.FAILED));
            outbox.write(Topics.PAYMENTS, event.orderId(), new PaymentFailed(
                    UUID.randomUUID(), event.orderId(), Instant.now(), "declined by PSP"));
        }
    }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) { /* other event types on subscribed topics: not ours */ }
}
