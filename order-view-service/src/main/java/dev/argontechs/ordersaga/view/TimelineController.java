package dev.argontechs.ordersaga.view;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class TimelineController {

    private final StreamsBuilderFactoryBean factory;

    public TimelineController(StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }

    @GetMapping("/orders/{id}/timeline")
    public ResponseEntity<Map<String, Object>> timeline(@PathVariable UUID id) {
        var streams = factory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        var store = streams.store(StoreQueryParameters.fromNameAndType(
                TopologyConfig.STORE, QueryableStoreTypes.<String, OrderTimeline>keyValueStore()));
        var timeline = store.get(id.toString());
        if (timeline == null) return ResponseEntity.notFound().build();

        var events = timeline.events.stream()
                .sorted(Comparator.comparing(TimelineEntry::occurredAt))
                .map(e -> {
                    var entry = new java.util.LinkedHashMap<String, Object>();
                    entry.put("type", e.type());
                    entry.put("occurredAt", e.occurredAt().toString());
                    entry.put("detail", e.detail());
                    return entry;
                })
                .toList();
        return ResponseEntity.ok(Map.of(
                "orderId", timeline.orderId,
                "status", timeline.status,
                "events", events));
    }
}
