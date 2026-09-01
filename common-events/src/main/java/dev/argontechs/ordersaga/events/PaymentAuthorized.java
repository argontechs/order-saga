package dev.argontechs.ordersaga.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record PaymentAuthorized(UUID eventId, UUID orderId, Instant occurredAt,
                                UUID paymentId, BigDecimal amount, List<OrderItem> items) {}
