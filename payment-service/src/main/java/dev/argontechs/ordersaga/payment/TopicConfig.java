package dev.argontechs.ordersaga.payment;

import dev.argontechs.ordersaga.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {
    @Bean NewTopic paymentsTopic() { return TopicBuilder.name(Topics.PAYMENTS).partitions(3).replicas(1).build(); }
}
