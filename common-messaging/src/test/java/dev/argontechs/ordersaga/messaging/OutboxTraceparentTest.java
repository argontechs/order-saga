package dev.argontechs.ordersaga.messaging;

import io.micrometer.tracing.CurrentTraceContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Traceparent continuity across the outbox hop: the writer captures the current
 * span's W3C traceparent at write time (scheduler-thread publish has no span of
 * its own to inherit from), and the publisher replays it as a record header so
 * the consumer-side observation can parent onto the original trace.
 */
@SuppressWarnings("unchecked")
class OutboxTraceparentTest {

    private static final String FIXED_TRACEPARENT = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    @Test
    void writerCapturesTraceparentWhenTracerAndPropagatorPresent() {
        var repo = mock(OutboxRepository.class);

        var tracer = mock(Tracer.class);
        var span = mock(Span.class);
        var traceContext = mock(TraceContext.class);
        var currentTraceContext = mock(CurrentTraceContext.class);
        var propagator = mock(Propagator.class);

        when(tracer.currentSpan()).thenReturn(span);
        when(tracer.currentTraceContext()).thenReturn(currentTraceContext);
        when(currentTraceContext.context()).thenReturn(traceContext);
        doAnswer(invocation -> {
            Map<String, String> carrier = invocation.getArgument(1);
            Propagator.Setter<Map<String, String>> setter = invocation.getArgument(2);
            setter.set(carrier, "traceparent", FIXED_TRACEPARENT);
            return null;
        }).when(propagator).inject(any(), any(), any());

        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        when(propagatorProvider.getIfAvailable()).thenReturn(propagator);

        var writer = new OutboxWriter(repo, new AvroPayloadCodec(), tracerProvider, propagatorProvider);
        var orderId = UUID.randomUUID();
        var event = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), orderId, Instant.now());

        writer.write("inventory.events", orderId, event);

        var captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(repo).save(captor.capture());
        assertThat(captor.getValue().getTraceparent()).isEqualTo(FIXED_TRACEPARENT);
    }

    @Test
    void writerLeavesTraceparentNullWhenNoTracerBeansPresent() {
        var repo = mock(OutboxRepository.class);
        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        // getIfAvailable() unstubbed -> Mockito default returns null, simulating no bean present.

        var writer = new OutboxWriter(repo, new AvroPayloadCodec(), tracerProvider, propagatorProvider);
        var orderId = UUID.randomUUID();
        var event = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), orderId, Instant.now());

        writer.write("inventory.events", orderId, event);

        var captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(repo).save(captor.capture());
        assertThat(captor.getValue().getTraceparent()).isNull();
    }

    @Test
    void publisherSendsTraceparentHeaderWhenPresentOnRow() {
        var repo = mock(OutboxRepository.class);
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        var codec = new AvroPayloadCodec();

        var originalEvent = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
        var json = codec.toJson(originalEvent);

        var row = new OutboxMessage(originalEvent.getEventId(), originalEvent.getOrderId(),
                "inventory.events", "dev.argontechs.ordersaga.events.InventoryReserved",
                json, Instant.now());
        row.setTraceparent(FIXED_TRACEPARENT);
        when(repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(List.of(row));
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        new OutboxPublisher(repo, template, codec, tracerProvider, propagatorProvider).publishPending();

        var recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(recordCaptor.capture());
        var sent = recordCaptor.getValue();
        assertThat(sent.topic()).isEqualTo("inventory.events");
        assertThat(sent.key()).isEqualTo(originalEvent.getOrderId().toString());
        assertThat(sent.value()).isEqualTo(originalEvent);
        var header = sent.headers().lastHeader("traceparent");
        assertThat(header).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(FIXED_TRACEPARENT);
        assertThat(row.getPublishedAt()).isNotNull();
        verify(repo).save(row);
    }

    @Test
    void publisherSendsNoHeaderWhenTraceparentAbsentOnRow() {
        var repo = mock(OutboxRepository.class);
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        var codec = new AvroPayloadCodec();

        var originalEvent = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
        var json = codec.toJson(originalEvent);

        var row = new OutboxMessage(originalEvent.getEventId(), originalEvent.getOrderId(),
                "inventory.events", "dev.argontechs.ordersaga.events.InventoryReserved",
                json, Instant.now());
        // no traceparent set
        when(repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(List.of(row));
        when(template.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        new OutboxPublisher(repo, template, codec, tracerProvider, propagatorProvider).publishPending();

        verify(template).send(eq("inventory.events"), eq(originalEvent.getOrderId().toString()), any());
        verify(template, never()).send(any(ProducerRecord.class));
        assertThat(row.getPublishedAt()).isNotNull();
        verify(repo).save(row);
    }

    /**
     * The critical continuity fix: with template.observation-enabled: true, KafkaTemplate's
     * own send-Observation unconditionally re-injects a traceparent header derived from
     * whatever trace is "current" on the publishing thread — which, on the @Scheduled
     * thread, is the scheduled task's own (unrelated) span, not the original request trace.
     * Left alone, that clobbers the header set above. The fix is to extract the stored
     * traceparent back into a real span and make it "current" for the duration of the send,
     * so the observation's own injection re-derives and writes back this same value instead
     * of the task's. This test pins that scope usage, independent of any real Observation
     * infrastructure.
     */
    @Test
    @SuppressWarnings("unchecked")
    void publisherEstablishesTracingScopeFromStoredTraceparentWhenTracerAndPropagatorPresent() {
        var repo = mock(OutboxRepository.class);
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        var codec = new AvroPayloadCodec();

        var tracer = mock(Tracer.class);
        var propagator = mock(Propagator.class);
        var spanBuilder = mock(Span.Builder.class);
        var span = mock(Span.class);
        var scope = mock(Tracer.SpanInScope.class);

        when(propagator.extract(any(), any())).thenReturn(spanBuilder);
        when(spanBuilder.name(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.start()).thenReturn(span);
        when(tracer.withSpan(span)).thenReturn(scope);

        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        when(propagatorProvider.getIfAvailable()).thenReturn(propagator);

        var originalEvent = new dev.argontechs.ordersaga.events.InventoryReserved(
                UUID.randomUUID(), UUID.randomUUID(), Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
        var json = codec.toJson(originalEvent);
        var row = new OutboxMessage(originalEvent.getEventId(), originalEvent.getOrderId(),
                "inventory.events", "dev.argontechs.ordersaga.events.InventoryReserved",
                json, Instant.now());
        row.setTraceparent(FIXED_TRACEPARENT);
        when(repo.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(List.of(row));
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        new OutboxPublisher(repo, template, codec, tracerProvider, propagatorProvider).publishPending();

        var carrierCaptor = ArgumentCaptor.forClass(Map.class);
        verify(propagator).extract(carrierCaptor.capture(), any());
        assertThat(carrierCaptor.getValue()).containsEntry("traceparent", FIXED_TRACEPARENT);
        verify(tracer).withSpan(span);
        verify(scope).close();
        verify(span).end();
        verify(template).send(any(ProducerRecord.class));
    }
}
