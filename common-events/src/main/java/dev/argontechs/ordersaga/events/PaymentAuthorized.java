package dev.argontechs.ordersaga.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

// PaymentAuthorized carries items forward so inventory-service needs no order lookup
// (event enrichment — trade-off documented in README)
public record PaymentAuthorized(UUID eventId, UUID orderId, Instant occurredAt,
                                UUID paymentId, BigDecimal amount, List<OrderItem> items) {}
