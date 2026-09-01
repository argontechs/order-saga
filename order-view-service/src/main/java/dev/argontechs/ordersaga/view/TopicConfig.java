package dev.argontechs.ordersaga.view;

import dev.argontechs.ordersaga.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Not the owner of these topics — declared defensively (idempotent create-if-missing)
 *  because Streams fails fast on missing source topics and auto-create is off. */
@Configuration
public class TopicConfig {
    @Bean NewTopic ordersTopic()    { return TopicBuilder.name(Topics.ORDERS).partitions(3).replicas(1).build(); }
    @Bean NewTopic paymentsTopic()  { return TopicBuilder.name(Topics.PAYMENTS).partitions(3).replicas(1).build(); }
    @Bean NewTopic inventoryTopic() { return TopicBuilder.name(Topics.INVENTORY).partitions(3).replicas(1).build(); }
    @Bean NewTopic shippingTopic()  { return TopicBuilder.name(Topics.SHIPPING).partitions(3).replicas(1).build(); }
}
