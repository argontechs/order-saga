package dev.argontechs.ordersaga.order;

import dev.argontechs.ordersaga.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {
    @Bean NewTopic ordersTopic() { return TopicBuilder.name(Topics.ORDERS).partitions(3).replicas(1).build(); }
}
