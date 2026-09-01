package dev.argontechs.ordersaga.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfig {

    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    static {
        kafka.start();
        postgres.start();
    }

    public static String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return postgres;
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return kafka;
    }
}
