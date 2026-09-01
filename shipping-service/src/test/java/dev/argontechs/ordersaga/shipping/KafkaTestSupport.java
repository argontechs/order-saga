package dev.argontechs.ordersaga.shipping;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@TestComponent
public class KafkaTestSupport {

    @Autowired KafkaProperties props;

    public String bootstrap() { return String.join(",", props.getBootstrapServers()); }

    private Map<String, Object> avroProps() {
        var cfg = new HashMap<String, Object>();
        cfg.put("bootstrap.servers", bootstrap());
        cfg.put("schema.registry.url", "mock://ordersaga");
        cfg.put("value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        cfg.put("specific.avro.reader", true);
        return cfg;
    }

    /** Send an event exactly like OutboxPublisher would: Avro value, orderId key. */
    public void send(String topic, UUID key, SpecificRecordBase event) {
        var serializer = new KafkaAvroSerializer();
        serializer.configure(avroProps(), false);
        try (var producer = new KafkaProducer<String, Object>(
                Map.of("bootstrap.servers", bootstrap()),
                new StringSerializer(), serializer)) {
            producer.send(new ProducerRecord<>(topic, key.toString(), event)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Poll records from topic (fresh group, from earliest), values deserialized to specific records. */
    public ConsumerRecords<String, Object> consume(String topic) {
        var deserializer = new KafkaAvroDeserializer();
        deserializer.configure(avroProps(), false);
        var cfg = new HashMap<String, Object>(avroProps());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<String, Object>(cfg, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
        }
    }
}
