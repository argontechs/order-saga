package dev.argontechs.ordersaga.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroPayloadCodec codec;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate, AvroPayloadCodec codec) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        for (var row : repository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            var event = codec.fromJson(row.getType(), row.getPayload());
            kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), event).join();
            row.markPublished();
            repository.save(row);
        }
    }
}
