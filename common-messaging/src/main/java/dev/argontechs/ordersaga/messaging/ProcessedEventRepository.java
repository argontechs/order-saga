package dev.argontechs.ordersaga.messaging;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import java.util.UUID;

public interface ProcessedEventRepository extends Repository<ProcessedEvent, UUID> {
    /** Atomic insert-if-absent — no exception on duplicate, so the surrounding
     *  transaction is never marked rollback-only. Returns 1 first time, 0 after. */
    @Modifying
    @Query(value = "INSERT INTO processed_events (id) VALUES (:id) ON CONFLICT DO NOTHING",
           nativeQuery = true)
    int insertIfAbsent(UUID id);
}
