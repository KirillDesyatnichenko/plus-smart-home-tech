package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConsumerFactory;
import ru.yandex.practicum.kafka.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerProperties;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioActionDecision;
import ru.yandex.practicum.telemetry.analyzer.service.ActionDispatcher;
import ru.yandex.practicum.telemetry.analyzer.service.ScenarioEvaluator;
import ru.yandex.practicum.telemetry.analyzer.storage.ScenarioStorage;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final AnalyzerProperties properties;
    private final KafkaConsumerFactory consumerFactory;
    private final ScenarioStorage storage;
    private final ScenarioEvaluator evaluator;
    private final ActionDispatcher dispatcher;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private static final int WARMUP_ATTEMPTS = 20;
    private static final long WARMUP_DELAY_MS = 250;

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("SnapshotProcessor уже запущен");
            return;
        }

        consumer = createConsumer();
        String topic = properties.getKafka().getTopics().getSnapshots();
        consumer.subscribe(Collections.singletonList(topic));
        log.info("SnapshotProcessor подписан на топик {}", topic);

        boolean autoCommit = properties.getKafka().getSnapshots().isEnableAutoCommit();
        Duration pollTimeout = Duration.ofMillis(
                Math.max(properties.getKafka().getSnapshots().getPollTimeoutMs(), 100)
        );

        try {
            while (running.get()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                processRecords(records);
                if (!autoCommit) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Получен сигнал на остановку SnapshotProcessor");
        } catch (Exception e) {
            log.error("Ошибка при обработке снапшотов", e);
        } finally {
            closeConsumer();
            running.set(false);
        }
    }

    public void stop() {
        running.set(false);
        Optional.ofNullable(consumer).ifPresent(KafkaConsumer::wakeup);
    }

    private KafkaConsumer<String, SensorsSnapshotAvro> createConsumer() {
        AnalyzerProperties.Consumer consumerProps = properties.getKafka().getSnapshots();
        return consumerFactory.create(SensorsSnapshotDeserializer.class, consumerProps, "telemetry-analyzer-snapshots");
    }

    private void processRecords(ConsumerRecords<String, SensorsSnapshotAvro> records) {
        for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
            SensorsSnapshotAvro snapshot = record.value();
            if (snapshot == null || snapshot.getHubId() == null) {
                continue;
            }
            try {
                log.debug("event=snapshot-received hubId={} partition={} offset={}",
                        snapshot.getHubId(), record.partition(), record.offset());
                processSnapshot(snapshot);
            } catch (Exception e) {
                log.error("Ошибка при обработке снапшота хаба {}", snapshot.getHubId(), e);
            }
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        List<Scenario> scenarios = loadScenariosWithWarmup(snapshot.getHubId());
        if (scenarios.isEmpty()) {
            log.debug("Для хаба {} нет сценариев, снапшот пропущен", snapshot.getHubId());
            return;
        }

        List<ScenarioActionDecision> decisions = evaluator.evaluate(snapshot, scenarios);
        if (decisions.isEmpty()) {
            log.debug("Ни одно условие не выполнено для хаба {}", snapshot.getHubId());
            return;
        }

        Instant timestamp = snapshot.getTimestamp();
        dispatcher.dispatch(snapshot.getHubId(), timestamp, decisions);
    }

    private List<Scenario> loadScenariosWithWarmup(String hubId) {
        List<Scenario> scenarios = storage.findByHubId(hubId);
        for (int i = 0; i < WARMUP_ATTEMPTS && scenarios.isEmpty(); i++) {
            try {
                Thread.sleep(WARMUP_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            scenarios = storage.findByHubId(hubId);
        }
        return scenarios;
    }

    private void closeConsumer() {
        try {
            if (consumer != null) {
                consumer.close(Duration.ofSeconds(5));
                log.info("SnapshotProcessor consumer закрыт");
            }
        } catch (Exception e) {
            log.warn("Не удалось корректно закрыть consumer снапшотов", e);
        } finally {
            consumer = null;
        }
    }
}