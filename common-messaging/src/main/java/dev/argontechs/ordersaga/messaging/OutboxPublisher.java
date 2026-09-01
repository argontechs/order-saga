package dev.argontechs.ordersaga.messaging;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroPayloadCodec codec;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate, AvroPayloadCodec codec,
                            ObjectProvider<Tracer> tracerProvider, ObjectProvider<Propagator> propagatorProvider) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        for (var row : repository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            var event = codec.fromJson(row.getType(), row.getPayload());
            // sync send: preserves per-aggregate ordering
            if (row.getTraceparent() != null) {
                sendWithTraceparent(row, event);
            } else {
                kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), event).join();
            }
            row.markPublished();
            repository.save(row);
        }
    }

    /**
     * Replays the traceparent captured at outbox-write time as a record header, so the
     * consumer-side spring-kafka observation can extract it and parent the new consumer
     * span onto the original trace — this is what makes one trace span the whole saga
     * despite the scheduler-thread outbox hop.
     *
     * <p>The header is set directly below as a baseline (and is all that's needed when
     * no tracer/propagator beans are present here). But when spring.kafka.template
     * .observation-enabled is on — as it is in every saga service — KafkaTemplate wraps
     * send() in its own Observation, which unconditionally re-injects a "traceparent"
     * header derived from whatever trace is *currently active on this thread*. Left
     * alone, that would silently overwrite our header with the @Scheduled task's own
     * (unrelated) span, breaking continuity right where it matters most. So we first
     * extract the stored traceparent back into a real span and make it "current" for
     * the duration of the send — the observation's own injection then re-derives and
     * writes back this same, correctly-parented value instead of clobbering it.
     */
    private void sendWithTraceparent(OutboxMessage row, Object event) {
        var record = new ProducerRecord<String, Object>(row.getTopic(), row.getAggregateId().toString(), event);
        record.headers().add("traceparent", row.getTraceparent().getBytes(StandardCharsets.UTF_8));

        var tracer = tracerProvider.getIfAvailable();
        var propagator = propagatorProvider.getIfAvailable();
        if (tracer != null && propagator != null) {
            var carrier = Map.of("traceparent", row.getTraceparent());
            var span = propagator.extract(carrier, Map::get).name("outbox-publish").start();
            try (var scope = tracer.withSpan(span)) {
                kafkaTemplate.send(record).join();
            } finally {
                span.end();
            }
        } else {
            kafkaTemplate.send(record).join();
        }
    }
}
