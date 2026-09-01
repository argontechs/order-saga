package dev.argontechs.ordersaga.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailed(UUID eventId, UUID orderId, Instant occurredAt, String reason) {}
