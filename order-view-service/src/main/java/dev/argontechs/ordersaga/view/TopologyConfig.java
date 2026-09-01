package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.Topics;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

@Configuration
@EnableKafkaStreams
public class TopologyConfig {

    public static final String STORE = "order-timelines";

    @Bean
    public KStream<String, SpecificRecordBase> timelineTopology(StreamsBuilder builder,
            @Value("${spring.kafka.properties.schema.registry.url}") String registryUrl) {

        var valueSerde = new SpecificAvroSerde<SpecificRecordBase>();
        valueSerde.configure(Map.of(
                "schema.registry.url", registryUrl,
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                "specific.avro.reader", "true"), false);

        var consumed = Consumed.with(Serdes.String(), valueSerde);
        var stream = builder.<String, SpecificRecordBase>stream(Topics.ORDERS, consumed)
                .merge(builder.stream(Topics.PAYMENTS, consumed))
                .merge(builder.stream(Topics.INVENTORY, consumed))
                .merge(builder.stream(Topics.SHIPPING, consumed));

        var aggregator = new TimelineAggregator();
        stream.groupByKey()
              .aggregate(OrderTimeline::new,
                         (key, event, timeline) -> aggregator.apply(key, event, timeline),
                         Materialized.<String, OrderTimeline, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE)
                                 .withKeySerde(Serdes.String())
                                 .withValueSerde(new JsonSerde<>(OrderTimeline.class)));
        return stream;
    }
}
