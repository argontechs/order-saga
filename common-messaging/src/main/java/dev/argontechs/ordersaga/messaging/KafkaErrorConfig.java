package dev.argontechs.ordersaga.messaging;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaErrorConfig {

    @Bean
    @SuppressWarnings("unchecked")
    public CommonErrorHandler kafkaErrorHandler(KafkaProperties props,
            @Value("${spring.kafka.consumer.group-id}") String group,
            @Value("${spring.kafka.properties.schema.registry.url}") String registryUrl) {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", props.getBootstrapServers()));
        var bytesTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerProps, new StringSerializer(), new ByteArraySerializer()));

        Map<String, Object> avroProps = new HashMap<>(producerProps);
        avroProps.put("schema.registry.url", registryUrl);
        avroProps.put("value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        var avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(avroProps, false);

        var avroTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerProps, new StringSerializer(), avroSerializer));

        var templates = new LinkedHashMap<Class<?>, KafkaOperations<?, ?>>();
        templates.put(byte[].class, bytesTemplate);   // deserialization poison → raw bytes
        templates.put(Object.class, avroTemplate);    // business failure → Avro-serialized

        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(group + ".DLT", record.partition()));

        var backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
