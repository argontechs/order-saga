package dev.argontechs.ordersaga.payment;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class DeadLetterIT extends AbstractKafkaIT {

    @Autowired KafkaTestSupport kafka;

    @Test
    void poisonMessageLandsInDltWithExceptionHeaders() throws Exception {
        // raw bytes with no valid Confluent magic byte/schema id → KafkaAvroDeserializer throws
        // → ErrorHandlingDeserializer → DLT. Uses a raw KafkaProducer instead of
        // KafkaTestSupport.send: send() serializes a real object via the configured Avro
        // serializer, but this test needs to put intentionally non-Avro bytes on the wire.
        try (var producer = new KafkaProducer<String, byte[]>(
                Map.of("bootstrap.servers", kafka.bootstrap()), new StringSerializer(), new ByteArraySerializer())) {
            var record = new ProducerRecord<>("orders.events", null, "poison-key",
                    "not avro at all".getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }

        // The DLT record's value is the original poison bytes republished verbatim (still not
        // valid Avro), so it must be read with a raw byte deserializer rather than
        // KafkaTestSupport.consume(), which configures a KafkaAvroDeserializer.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var cfg = KafkaTestUtils.consumerProps(kafka.bootstrap(), "test-" + UUID.randomUUID(), "true");
            cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            try (var consumer = new KafkaConsumer<String, byte[]>(
                    cfg, new StringDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(List.of("payment-service.DLT"));
                var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
                assertThat(records).anySatisfy(r -> {
                    assertThat(r.key()).isEqualTo("poison-key");
                    assertThat(r.headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
                });
            }
        });
    }
}
