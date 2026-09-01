package dev.argontechs.ordersaga.shipping;

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
 * equal, and Spring boots a brand new {@code ApplicationContext} per class instead
 * of reusing a cached one.
 *
 * <p><b>In this module specifically</b>, that context-cache reuse never actually
 * happens between {@code ShippingIT.HappyPath} and {@code ShippingIT.ForcedFailure}:
 * {@code ForcedFailure} carries its own {@code @TestPropertySource(properties =
 * "shipping.failure-rate=1.0")}, which gives it a different
 * {@code MergedContextConfiguration} from {@code HappyPath} regardless of shared
 * {@code @DynamicPropertySource} method identity — so the two always get separate
 * contexts, each with its own live {@code @KafkaListener} on consumer group
 * {@code shipping-service}. This base class is kept anyway for consistency with
 * sibling services (payment-service) and because it still matters within a group of
 * classes that *do* share the same test properties (avoids needlessly re-forking a
 * context among those). The actual flake guard for the cross-class listener race
 * here is {@code @DirtiesContext(classMode = ClassMode.AFTER_CLASS)} on both nested
 * classes in {@code ShippingIT}: it tears down each class's context — and with it,
 * that class's live listener — before the next class starts, so only one listener on
 * consumer group {@code shipping-service} is ever live at a time. Do not remove it.
 */
abstract class AbstractKafkaIT {

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", TestcontainersConfig::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", () -> "mock://ordersaga");
    }
}
