package dev.argontechs.ordersaga.messaging;

import org.apache.avro.specific.SpecificRecordBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {

    @Test
    @SuppressWarnings("unchecked")
    void publishesPendingRowsAndMarksThem() {
        var repo = mock(OutboxRepository.class);
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        var codec = new AvroPayloadCodec();

        // Create a real event and encode it as JSON
        var originalEvent = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
        var json = codec.toJson(originalEvent);

        var row = new OutboxMessage(originalEvent.getEventId(), originalEvent.getOrderId(),
                "inventory.events", "dev.argontechs.ordersaga.events.InventoryReserved",
                json, Instant.now());
        when(repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(List.of(row));
        when(template.send(anyString(), anyString(), any(SpecificRecordBase.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        new OutboxPublisher(repo, template, codec).publishPending();

        var topicCaptor = ArgumentCaptor.forClass(String.class);
        var keyCaptor = ArgumentCaptor.forClass(String.class);
        var eventCaptor = ArgumentCaptor.forClass(SpecificRecordBase.class);
        verify(template).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo("inventory.events");
        assertThat(keyCaptor.getValue()).isEqualTo(originalEvent.getOrderId().toString());
        assertThat(eventCaptor.getValue()).isEqualTo(originalEvent);
        assertThat(row.getPublishedAt()).isNotNull();
        verify(repo).save(row);
    }
}
