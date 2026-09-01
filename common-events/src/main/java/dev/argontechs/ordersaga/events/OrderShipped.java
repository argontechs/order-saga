package dev.argontechs.ordersaga.events;

import java.time.Instant;
import java.util.UUID;

public record OrderShipped(UUID eventId, UUID orderId, Instant occurredAt, UUID shipmentId) {}
