package dev.argontechs.ordersaga.messaging;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final AvroPayloadCodec codec;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public OutboxWriter(OutboxRepository repository, AvroPayloadCodec codec,
                         ObjectProvider<Tracer> tracerProvider,
                         ObjectProvider<Propagator> propagatorProvider) {
        this.repository = repository;
        this.codec = codec;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    /** Must be called inside the caller's transaction — that IS the outbox pattern. */
    public void write(String topic, UUID aggregateId, SpecificRecordBase event) {
        try {
            var eventId = (UUID) event.getClass().getMethod("getEventId").invoke(event);
            var row = new OutboxMessage(eventId, aggregateId, topic,
                    event.getClass().getName(), codec.toJson(event), Instant.now());

            // Capture the current span's W3C traceparent at write time. The scheduler
            // thread that later publishes this row has no span of its own to inherit
            // from, so the trace would otherwise break at the outbox hop; stashing it
            // here lets OutboxPublisher replay it as a record header, and the
            // consumer-side observation parents its span onto this original trace.
            var tracer = tracerProvider.getIfAvailable();
            var propagator = propagatorProvider.getIfAvailable();
            if (tracer != null && propagator != null && tracer.currentSpan() != null) {
                var carrier = new HashMap<String, String>();
                propagator.inject(tracer.currentTraceContext().context(), carrier, Map::put);
                row.setTraceparent(carrier.get("traceparent"));
            }

            repository.save(row);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to write outbox event", e);
        }
    }
}
