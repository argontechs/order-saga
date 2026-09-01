package dev.argontechs.ordersaga.messaging;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {

    @Test
    @SuppressWarnings("unchecked")
    void publishesPendingRowsAndMarksThem() {
        var repo = mock(OutboxRepository.class);
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        var row = new OutboxMessage(UUID.randomUUID(), UUID.randomUUID(),
                "orders.events", "dev.argontechs.ordersaga.events.OrderCreated",
                "{\"x\":1}", Instant.now());
        when(repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(List.of(row));
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        new OutboxPublisher(repo, template).publishPending();

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        ProducerRecord<String, String> sent = captor.getValue();
        assertThat(sent.topic()).isEqualTo("orders.events");
        assertThat(sent.key()).isEqualTo(row.getAggregateId().toString());
        assertThat(sent.value()).isEqualTo("{\"x\":1}");
        assertThat(new String(sent.headers().lastHeader("__TypeId__").value()))
                .isEqualTo("dev.argontechs.ordersaga.events.OrderCreated");
        assertThat(row.getPublishedAt()).isNotNull();
        verify(repo).save(row);
    }
}
