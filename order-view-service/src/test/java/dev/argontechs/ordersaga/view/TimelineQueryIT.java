package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TimelineQueryIT.Containers.class)
class TimelineQueryIT {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");
        static { kafka.start(); }

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() { return kafka; }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", Containers.kafka::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", () -> "mock://ordersaga");
        registry.add("spring.kafka.streams.properties.state.dir",
                () -> System.getProperty("java.io.tmpdir") + "/kstreams-test-" + UUID.randomUUID());
    }

    @Autowired TestRestTemplate rest;

    private void send(String topic, UUID orderId, Object event) {
        var serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(
                "bootstrap.servers", Containers.kafka.getBootstrapServers(),
                "schema.registry.url", "mock://ordersaga",
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy"), false);
        try (var producer = new KafkaProducer<String, Object>(
                Map.of("bootstrap.servers", Containers.kafka.getBootstrapServers()),
                new StringSerializer(), serializer)) {
            producer.send(new ProducerRecord<>(topic, orderId.toString(), event)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void servesTimelineForOrderAndNotFoundForUnknown() {
        var orderId = UUID.randomUUID();
        var items = List.of(new OrderItem("P100", 1, new BigDecimal("10.00")));
        send(Topics.ORDERS, orderId, new OrderCreated(UUID.randomUUID(), orderId, Instant.now(), "c1", items, new BigDecimal("10.00")));
        send(Topics.PAYMENTS, orderId, new PaymentAuthorized(UUID.randomUUID(), orderId, Instant.now(), UUID.randomUUID(), new BigDecimal("10.00"), items));

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            var response = rest.getForEntity("/orders/" + orderId + "/timeline", Map.class);
            assertThat(response.getStatusCode().value()).isEqualTo(200);
            assertThat(response.getBody().get("status")).isEqualTo("PAID");
            assertThat((List<Map<String, Object>>) response.getBody().get("events")).hasSize(2);
        });

        assertThat(rest.getForEntity("/orders/" + UUID.randomUUID() + "/timeline", Map.class)
                .getStatusCode().value()).isEqualTo(404);
    }
}
