package dev.argontechs.ordersaga.inventory;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Shared base for Kafka-backed integration tests.
 *
 * <p>The {@code @DynamicPropertySource} method must live in a single shared
 * declaring class. Spring's {@code DynamicPropertiesContextCustomizer} keys context
 * caching off a {@code Set<Method>}, and {@link java.lang.reflect.Method#equals}
 * compares declaring class too — so two IT classes each declaring their own
 * (identical-looking) {@code @DynamicPropertySource} method are never considered
 * equal, and Spring boots a brand new {@code ApplicationContext} (with its own
 * real {@code @KafkaListener}) per class. Two such contexts then fight over the
 * same consumer group / single-partition topic, causing flaky partition
 * assignment across test classes. Extending this common base keeps the
 * {@code Method} identity shared, so the context (and its listener) is cached
 * and reused across all Kafka IT classes in this module.
 */
abstract class AbstractKafkaIT {

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", TestcontainersConfig::getBootstrapServers);
    }
}
