package dev.argontechs.ordersaga.view;

import java.time.Instant;

public record TimelineEntry(String type, Instant occurredAt, String detail) {}
