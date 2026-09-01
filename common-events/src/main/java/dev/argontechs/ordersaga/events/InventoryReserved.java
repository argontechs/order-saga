package dev.argontechs.ordersaga.events;

import java.time.Instant;
import java.util.UUID;

public record InventoryReserved(UUID eventId, UUID orderId, Instant occurredAt) {}
