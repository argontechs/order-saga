package dev.argontechs.ordersaga.view;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Mutable aggregate held in the Streams state store (JSON-serialized). */
public class OrderTimeline {
    public String orderId;
    public String status = "PENDING";
    public Set<String> eventIds = new HashSet<>();
    public List<TimelineEntry> events = new ArrayList<>();
}
