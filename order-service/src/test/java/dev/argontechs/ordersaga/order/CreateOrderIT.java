package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.messaging.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({TestcontainersConfig.class, KafkaTestSupport.class})
class CreateOrderIT extends AbstractKafkaIT {

    @Autowired TestRestTemplate rest;
    @Autowired OrderRepository orders;
    @Autowired OutboxRepository outbox;

    @Test
    void createOrderPersistsOrderAndOutboxEventAtomically() {
        var body = Map.of(
                "customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 2, "unitPrice", 49.90)));

        var response = rest.postForEntity("/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        var orderId = UUID.fromString((String) response.getBody().get("orderId"));

        var order = orders.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getTotalAmount()).isEqualByComparingTo(new BigDecimal("99.80"));

        assertThat(outbox.findAll())
                .anySatisfy(row -> {
                    assertThat(row.getAggregateId()).isEqualTo(orderId);
                    assertThat(row.getTopic()).isEqualTo("orders.events");
                    assertThat(row.getType()).endsWith("OrderCreated");
                });
    }

    @Test
    void rejectsEmptyItems() {
        var response = rest.postForEntity("/orders",
                Map.of("customerId", "cust-1", "items", java.util.List.of()), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsDuplicateProductIdInItems() {
        var body = Map.of("customerId", "cust-1", "items", java.util.List.of(
                Map.of("productId", "P100", "quantity", 1, "unitPrice", 10.00),
                Map.of("productId", "P100", "quantity", 2, "unitPrice", 5.00)));

        var response = rest.postForEntity("/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsZeroQuantity() {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 0, "unitPrice", 10.00)));

        var response = rest.postForEntity("/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsNegativeQuantity() {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", -1, "unitPrice", 10.00)));

        var response = rest.postForEntity("/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsZeroUnitPrice() {
        var body = Map.of("customerId", "cust-1",
                "items", java.util.List.of(Map.of("productId", "P100", "quantity", 1, "unitPrice", 0.00)));

        var response = rest.postForEntity("/orders", body, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
