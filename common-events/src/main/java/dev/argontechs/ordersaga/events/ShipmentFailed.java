package dev.argontechs.ordersaga.events;

import java.time.Instant;
import java.util.UUID;

public record ShipmentFailed(UUID eventId, UUID orderId, Instant occurredAt, String reason) {}
