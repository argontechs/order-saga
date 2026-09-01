package dev.argontechs.ordersaga.messaging;

import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class IdempotencyGuard {

    private final ProcessedEventRepository repository;

    public IdempotencyGuard(ProcessedEventRepository repository) {
        this.repository = repository;
    }

    /** True exactly once per eventId. Call first, inside the listener's transaction. */
    public boolean firstTime(UUID eventId) {
        return repository.insertIfAbsent(eventId) == 1;
    }
}
