package dev.argontechs.ordersaga.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OutboxWriterTest {

    record FakeEvent(UUID eventId, String name) {}

    @Test
    void writesSerializedEventRow() {
        var repo = mock(OutboxRepository.class);
        var writer = new OutboxWriter(repo, new ObjectMapper().registerModule(new JavaTimeModule()));
        var event = new FakeEvent(UUID.randomUUID(), "hello");
        var orderId = UUID.randomUUID();

        writer.write("orders.events", orderId, event);

        var captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(repo).save(captor.capture());
        var row = captor.getValue();
        assertThat(row.getId()).isEqualTo(event.eventId());
        assertThat(row.getAggregateId()).isEqualTo(orderId);
        assertThat(row.getTopic()).isEqualTo("orders.events");
        assertThat(row.getType()).isEqualTo(FakeEvent.class.getName());
        assertThat(row.getPayload()).contains("\"hello\"");
        assertThat(row.getPublishedAt()).isNull();
    }
}
