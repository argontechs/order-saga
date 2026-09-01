package dev.argontechs.ordersaga.events;

import java.time.Instant;
import java.util.UUID;

public record OutOfStock(UUID eventId, UUID orderId, Instant occurredAt, String productId) {}
