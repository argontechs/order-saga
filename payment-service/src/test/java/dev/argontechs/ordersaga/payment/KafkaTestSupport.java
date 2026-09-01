package dev.argontechs.ordersaga.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@TestComponent
public class KafkaTestSupport {

    @Autowired KafkaProperties props;
    final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    String bootstrap() { return String.join(",", props.getBootstrapServers()); }

    /** Send an event exactly like OutboxPublisher would: JSON value, orderId key, __TypeId__ header. */
    public void send(String topic, UUID key, Object event) {
        try (var producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", bootstrap()), new StringSerializer(), new StringSerializer())) {
            var record = new ProducerRecord<>(topic, null, key.toString(), mapper.writeValueAsString(event));
            record.headers().add("__TypeId__", event.getClass().getName().getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Poll one record from topic (fresh group, from earliest). */
    public ConsumerRecords<String, String> consume(String topic) {
        var cfg = KafkaTestUtils.consumerProps(bootstrap(), "test-" + UUID.randomUUID(), "true");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<String, String>(cfg, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
        }
    }
}
