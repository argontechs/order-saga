package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.*;
import org.apache.avro.specific.SpecificRecordBase;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Folds saga events into the per-order timeline. Idempotent by eventId
 *  (Streams is at-least-once; a reprocessed record must not duplicate entries). */
public class TimelineAggregator {

    private static final List<String> FORWARD = List.of("PENDING", "PAID", "RESERVED", "CONFIRMED");

    public OrderTimeline apply(String orderIdKey, SpecificRecordBase event, OrderTimeline timeline) {
        var eventId = ((UUID) event.get("eventId")).toString();
        if (!timeline.eventIds.add(eventId)) return timeline;

        timeline.orderId = orderIdKey;
        var occurredAt = (Instant) event.get("occurredAt");
        String detail = null;

        switch (event) {
            case OrderCreated e -> advance(timeline, "PENDING");
            case PaymentAuthorized e -> advance(timeline, "PAID");
            case InventoryReserved e -> advance(timeline, "RESERVED");
            case OrderShipped e -> advance(timeline, "CONFIRMED");
            case PaymentFailed e -> { detail = e.getReason(); cancel(timeline, detail); }
            case OutOfStock e -> { detail = "out of stock: " + e.getProductId(); cancel(timeline, detail); }
            case ShipmentFailed e -> { detail = e.getReason(); cancel(timeline, detail); }
            case PaymentRefunded e -> detail = "refund issued";
            case InventoryReleased e -> detail = "stock released";
            default -> { }
        }
        timeline.events.add(new TimelineEntry(event.getClass().getSimpleName(), occurredAt, detail));
        return timeline;
    }

    private void advance(OrderTimeline t, String next) {
        if (t.status.equals("CANCELLED") || t.status.equals("CONFIRMED")) return;
        if (FORWARD.indexOf(next) > FORWARD.indexOf(t.status)) t.status = next;
        // OrderCreated on a fresh timeline: indexOf("PENDING") == indexOf("PENDING"), stays PENDING — fine.
    }

    private void cancel(OrderTimeline t, String reason) {
        if (t.status.equals("CONFIRMED") || t.status.equals("CANCELLED")) return;
        t.status = "CANCELLED";
    }
}
