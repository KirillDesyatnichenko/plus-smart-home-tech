package ru.yandex.practicum.telemetry.analyzer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerProperties.Consumer;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerProperties.Kafka;

import java.util.Objects;
import java.util.Properties;

@Component
@RequiredArgsConstructor
public class KafkaConsumerFactory {

    private final AnalyzerProperties properties;

    public <T> KafkaConsumer<String, T> create(Class<? extends Deserializer<T>> valueDeserializer,
                                               Consumer consumerProps,
                                               String defaultGroupId) {
        Kafka kafkaProps = properties.getKafka();

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNullElse(consumerProps.getGroupId(), defaultGroupId));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerProps.isEnableAutoCommit());
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.putAll(consumerProps.getProperties());

        return new KafkaConsumer<>(config);
    }
}