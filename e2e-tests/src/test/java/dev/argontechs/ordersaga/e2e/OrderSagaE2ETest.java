package dev.argontechs.ordersaga.e2e;

import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Black-box suite. Requires the full system running:
 *  mvn -DskipTests package && docker compose --profile app up --build -d
 *  then: mvn -pl e2e-tests test -De2e
 *
 *  The app services declare no healthchecks, so `up -d` (or `--wait`) returns before Spring Boot
 *  has finished booting. createOrder() tolerates that cold start by retrying connection failures
 *  for up to 90s instead of requiring the caller to wait first. */
class OrderSagaE2ETest {

    final RestClient client = RestClient.create("http://localhost:8081");
    final RestClient viewClient = RestClient.create("http://localhost:8086");

    private String createOrder(String productId, int quantity, double unitPrice) {
        AtomicReference<String> orderId = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> orderId.set(doCreateOrder(productId, quantity, unitPrice)));
        return orderId.get();
    }

    @SuppressWarnings("unchecked")
    private String doCreateOrder(String productId, int quantity, double unitPrice) {
        Map<String, Object> response = client.post().uri("/orders")
                .body(Map.of("customerId", "e2e-cust",
                        "items", List.of(Map.of("productId", productId, "quantity", quantity, "unitPrice", unitPrice))))
                .retrieve().body(Map.class);
        return (String) response.get("orderId");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getOrder(String orderId) {
        return client.get().uri("/orders/" + orderId).retrieve().body(Map.class);
    }

    private void awaitStatus(String orderId, String expected) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(getOrder(orderId).get("status")).isEqualTo(expected));
    }

    @SuppressWarnings("unchecked")
    private void awaitTimeline(String orderId) {
        // order-view-service builds its state store by consuming all four topics independently of
        // order-service's own view, so it can still be warming up (or briefly 503) after order-service
        // already reports CONFIRMED — same cold-start tolerance idiom as createOrder().
        AtomicReference<Map<String, Object>> timeline = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> timeline.set(viewClient.get().uri("/orders/" + orderId + "/timeline")
                        .retrieve().body(Map.class)));

        var body = timeline.get();
        assertThat(body.get("status")).isEqualTo("CONFIRMED");
        var events = (List<Map<String, Object>>) body.get("events");
        assertThat(events.size()).isGreaterThanOrEqualTo(4);
        var types = events.stream().map(e -> (String) e.get("type")).toList();
        assertThat(types).contains("OrderCreated", "PaymentAuthorized", "InventoryReserved", "OrderShipped");
    }

    @Test
    void happyPathOrderIsConfirmed() {
        var orderId = createOrder("P100", 2, 49.90);
        awaitStatus(orderId, "CONFIRMED");
        awaitTimeline(orderId);
    }

    @Test
    void expensiveOrderIsDeclinedAndCancelled() {
        var orderId = createOrder("P100", 3, 5000.00); // 15000 >= 10000 → PSP declines
        awaitStatus(orderId, "CANCELLED");
        assertThat(getOrder(orderId).get("cancellationReason")).isEqualTo("declined by PSP");
    }

    @Test
    void outOfStockOrderIsRefundedAndCancelled() {
        var orderId = createOrder("P200", 50, 10.00); // 500 total passes PSP; only 5 in stock
        awaitStatus(orderId, "CANCELLED");
        assertThat((String) getOrder(orderId).get("cancellationReason")).startsWith("out of stock");
    }
}
