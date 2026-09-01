package dev.argontechs.ordersaga.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    public OutboxWriter(OutboxRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /** Must be called inside the caller's transaction — that IS the outbox pattern. */
    public void write(String topic, UUID aggregateId, Object event) {
        try {
            var eventId = (UUID) event.getClass().getMethod("eventId").invoke(event);
            repository.save(new OutboxMessage(eventId, aggregateId, topic,
                    event.getClass().getName(), mapper.writeValueAsString(event), Instant.now()));
        } catch (JsonProcessingException | ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to write outbox event", e);
        }
    }
}
