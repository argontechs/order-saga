package dev.argontechs.ordersaga.payment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class DeadLetterIT extends AbstractKafkaIT {

    @Autowired KafkaTestSupport kafka;

    @Test
    void poisonMessageLandsInDltWithExceptionHeaders() throws Exception {
        // claims to be OrderCreated but body is not valid JSON for it → deserialization poison
        // Uses a raw KafkaProducer instead of KafkaTestSupport.send: send() serializes a real
        // object via the configured JSON serializer, but this test needs to put intentionally
        // malformed JSON on the wire.
        try (var producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafka.bootstrap()), new StringSerializer(), new StringSerializer())) {
            var record = new ProducerRecord<>("orders.events", null, "poison-key", "{not json at all");
            record.headers().add("__TypeId__",
                    "dev.argontechs.ordersaga.events.OrderCreated".getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var records = kafka.consume("payment-service.DLT");
            assertThat(records).anySatisfy(r -> {
                assertThat(r.key()).isEqualTo("poison-key");
                assertThat(r.headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
            });
        });
    }
}
