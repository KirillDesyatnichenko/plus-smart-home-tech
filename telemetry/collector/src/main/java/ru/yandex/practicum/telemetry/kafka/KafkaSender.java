package ru.yandex.practicum.telemetry.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.serializer.AvroSerializer;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Service
public class KafkaSender {

    private final Producer<String, SpecificRecordBase> producer;

    public KafkaSender(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                AvroSerializer.class.getName());

        this.producer = new KafkaProducer<>(config);
    }

    public void send(String topic, SpecificRecordBase record) {
        try {
            producer.send(new ProducerRecord<>(topic, record));
            log.info("Сообщение успешно отправлено в топик {}", topic);
        } catch (Exception e) {
            throw new RuntimeException("Не удалось отправить сообщение в топик " + topic, e);
        }
    }

    @PreDestroy
    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofMillis(10));
            log.info("Kafka producer закрыт");
        } catch (Exception e) {
            log.warn("Ошибка при закрытии Kafka producer", e);
        }
    }
}