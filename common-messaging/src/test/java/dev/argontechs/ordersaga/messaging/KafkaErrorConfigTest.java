package dev.argontechs.ordersaga.messaging;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaErrorConfigTest {

    @Test
    void countingRecovererDelegatesToDlqRecovererThenIncrementsCounter() {
        var registry = new SimpleMeterRegistry();
        ConsumerRecordRecoverer dlqRecoverer = mock(ConsumerRecordRecoverer.class);
        var counted = KafkaErrorConfig.countingRecoverer(dlqRecoverer, registry, "payment-service");

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("payment.events", 0, 0L, "key", "value");
        var ex = new RuntimeException("boom");
        counted.accept(record, ex);

        assertThat(registry.counter("ordersaga.dlt.messages", "group", "payment-service").count())
                .isEqualTo(1.0);
        var order = inOrder(dlqRecoverer);
        order.verify(dlqRecoverer).accept(record, ex);
    }

    @Test
    void countingRecovererDoesNotIncrementWhenDlqRecovererThrows() {
        var registry = new SimpleMeterRegistry();
        ConsumerRecordRecoverer dlqRecoverer = mock(ConsumerRecordRecoverer.class);
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("payment.events", 0, 0L, "key", "value");
        var ex = new RuntimeException("boom");
        doThrow(new RuntimeException("dlt publish failed")).when(dlqRecoverer).accept(record, ex);
        var counted = KafkaErrorConfig.countingRecoverer(dlqRecoverer, registry, "payment-service");

        assertThatThrownBy(() -> counted.accept(record, ex)).hasMessage("dlt publish failed");

        assertThat(registry.counter("ordersaga.dlt.messages", "group", "payment-service").count())
                .isEqualTo(0.0);
        verify(dlqRecoverer).accept(record, ex);
    }

    @Test
    void countingRecovererIncrementsSeparatelyPerGroup() {
        var registry = new SimpleMeterRegistry();
        ConsumerRecordRecoverer dlqRecoverer = mock(ConsumerRecordRecoverer.class);
        var paymentCounted = KafkaErrorConfig.countingRecoverer(dlqRecoverer, registry, "payment-service");
        var orderCounted = KafkaErrorConfig.countingRecoverer(dlqRecoverer, registry, "order-service");

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("t", 0, 0L, "key", "value");
        var ex = new RuntimeException("boom");
        paymentCounted.accept(record, ex);
        paymentCounted.accept(record, ex);
        orderCounted.accept(record, ex);

        assertThat(registry.counter("ordersaga.dlt.messages", "group", "payment-service").count())
                .isEqualTo(2.0);
        assertThat(registry.counter("ordersaga.dlt.messages", "group", "order-service").count())
                .isEqualTo(1.0);
    }
}
