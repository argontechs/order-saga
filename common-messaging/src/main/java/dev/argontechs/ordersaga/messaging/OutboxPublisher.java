package dev.argontechs.ordersaga.messaging;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

@Component
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        for (var row : repository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            var record = new ProducerRecord<>(row.getTopic(), null,
                    row.getAggregateId().toString(), row.getPayload());
            record.headers().add("__TypeId__", row.getType().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).join(); // sync: preserve per-aggregate ordering
            row.markPublished();
            repository.save(row);
        }
    }
}
