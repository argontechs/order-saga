package dev.argontechs.ordersaga.messaging;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OutboxWriterTest {

    @Test
    @SuppressWarnings("unchecked")
    void writesAvroJsonRow() {
        var repo = mock(OutboxRepository.class);
        // No tracer/propagator beans present (the common case for a module with no
        // tracing auto-config on the classpath) — write() must succeed with a null
        // traceparent, exercised explicitly in OutboxTraceparentTest.
        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        var writer = new OutboxWriter(repo, new AvroPayloadCodec(), tracerProvider, propagatorProvider);
        var orderId = UUID.randomUUID();
        var event = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), orderId, Instant.now());

        writer.write("inventory.events", orderId, event);

        var captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(repo).save(captor.capture());
        var row = captor.getValue();
        assertThat(row.getId()).isEqualTo(event.getEventId());
        assertThat(row.getType()).isEqualTo("dev.argontechs.ordersaga.events.InventoryReserved");
        assertThat(row.getAggregateId()).isEqualTo(orderId);
        assertThat(row.getTopic()).isEqualTo("inventory.events");
        assertThat(row.getPayload()).contains(orderId.toString());
        assertThat(row.getPublishedAt()).isNull();
    }

    @Test
    void codecRoundTripPreservesEvent() {
        var codec = new AvroPayloadCodec();
        var originalEvent = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));

        var json = codec.toJson(originalEvent);
        var reconstructed = codec.fromJson("dev.argontechs.ordersaga.events.InventoryReserved", json);

        assertThat(reconstructed).isEqualTo(originalEvent);
    }
}
