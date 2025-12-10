package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.serializer.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.AggregatorProperties;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final AggregatorProperties properties;
    private final SensorsSnapshotAggregator snapshotAggregator;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, SensorsSnapshotAvro> producer;

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Агрегатор уже запущен");
            return;
        }

        consumer = createConsumer();
        producer = createProducer();

        String sensorsTopic = properties.getKafka().getSensorsTopic();
        consumer.subscribe(List.of(sensorsTopic));
        log.info("Агрегатор подписан на топик {} (pollTimeoutMs={})", sensorsTopic, properties.getPollTimeoutMs());

        try {
            Duration pollTimeout = Duration.ofMillis(properties.getPollTimeoutMs());
            while (running.get()) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                processRecords(records);
                producer.flush();
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Получен сигнал на остановку агрегатора");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            closeResources();
            running.set(false);
        }
    }

    public void stop() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void processRecords(ConsumerRecords<String, SensorEventAvro> records) {
        for (ConsumerRecord<String, SensorEventAvro> record : records) {
            SensorEventAvro event = record.value();
            if (event == null) continue;

            Optional<SensorsSnapshotAvro> snapshotOptional = snapshotAggregator.updateState(event);
            log.debug("event=sensor-event-received hubId={} sensorId={} partition={} offset={}",
                    event.getHubId(), event.getId(), record.partition(), record.offset());
            snapshotOptional.ifPresent(this::publishSnapshot);
        }
    }

    private void publishSnapshot(SensorsSnapshotAvro snapshot) {
        String topic = properties.getKafka().getSnapshotsTopic();
        ProducerRecord<String, SensorsSnapshotAvro> record =
                new ProducerRecord<>(topic, snapshot.getHubId(), snapshot);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить снапшот хаба {} в топик {}", snapshot.getHubId(), topic, exception);
            } else if (metadata != null) {
                log.info("event=snapshot-produced hubId={} topic={} partition={} offset={}",
                        snapshot.getHubId(), metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private KafkaConsumer<String, SensorEventAvro> createConsumer() {
        Properties config = new Properties();
        AggregatorProperties.Kafka kafka = properties.getKafka();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.getGroupId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(config);
    }

    private KafkaProducer<String, SensorsSnapshotAvro> createProducer() {
        Properties config = new Properties();
        AggregatorProperties.Kafka kafka = properties.getKafka();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(config);
    }

    private void closeResources() {
        try {
            if (producer != null) {
                producer.flush();
            }
        } catch (Exception e) {
            log.warn("Не удалось сбросить буфер продюсера перед остановкой", e);
        }

        try {
            if (consumer != null) {
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.warn("Не удалось зафиксировать смещения перед остановкой", e);
        }

        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            log.warn("Ошибка при закрытии консьюмера", e);
        }

        try {
            if (producer != null) {
                producer.close(Duration.ofSeconds(5));
            }
        } catch (Exception e) {
            log.warn("Ошибка при закрытии продюсера", e);
        }

        consumer = null;
        producer = null;
    }
}