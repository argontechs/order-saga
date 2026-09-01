package dev.argontechs.ordersaga.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void orderCreatedRoundTrips() throws Exception {
        var event = new OrderCreated(UUID.randomUUID(), UUID.randomUUID(), Instant.now(),
                "cust-1", List.of(new OrderItem("P100", 2, new BigDecimal("49.90"))),
                new BigDecimal("99.80"));
        var json = mapper.writeValueAsString(event);
        assertThat(mapper.readValue(json, OrderCreated.class)).isEqualTo(event);
    }

    @Test
    void paymentAuthorizedRoundTrips() throws Exception {
        var event = new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), Instant.now(),
                UUID.randomUUID(), new BigDecimal("99.80"),
                List.of(new OrderItem("P100", 2, new BigDecimal("49.90"))));
        var json = mapper.writeValueAsString(event);
        assertThat(mapper.readValue(json, PaymentAuthorized.class)).isEqualTo(event);
    }
}
