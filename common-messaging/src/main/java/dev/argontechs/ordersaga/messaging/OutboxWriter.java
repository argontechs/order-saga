package dev.argontechs.ordersaga.messaging;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final AvroPayloadCodec codec;

    public OutboxWriter(OutboxRepository repository, AvroPayloadCodec codec) {
        this.repository = repository;
        this.codec = codec;
    }

    /** Must be called inside the caller's transaction — that IS the outbox pattern. */
    public void write(String topic, UUID aggregateId, SpecificRecordBase event) {
        try {
            var eventId = (UUID) event.getClass().getMethod("getEventId").invoke(event);
            repository.save(new OutboxMessage(eventId, aggregateId, topic,
                    event.getClass().getName(), codec.toJson(event), Instant.now()));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to write outbox event", e);
        }
    }
}
