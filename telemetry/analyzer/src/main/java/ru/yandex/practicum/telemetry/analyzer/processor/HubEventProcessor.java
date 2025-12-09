package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConsumerFactory;
import ru.yandex.practicum.kafka.deserializer.HubEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerProperties;
import ru.yandex.practicum.telemetry.analyzer.storage.ScenarioStorage;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final AnalyzerProperties properties;
    private final KafkaConsumerFactory consumerFactory;
    private final ScenarioStorage storage;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, HubEventAvro> consumer;

    @Override
    public void run() {
        start();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Обработчик событий хаба уже запущен");
            return;
        }

        consumer = createConsumer();
        String topic = properties.getKafka().getTopics().getHubEvents();
        consumer.subscribe(Collections.singletonList(topic));
        log.info("HubEventProcessor подписан на топик {} (autoCommit={})",
                topic, properties.getKafka().getHubEvents().isEnableAutoCommit());

        try {
            Duration pollTimeout = Duration.ofMillis(
                    Math.max(properties.getKafka().getHubEvents().getPollTimeoutMs(), 100)
            );
            while (running.get()) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                processRecords(records);
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.info("Получен сигнал на остановку обработчика событий хаба");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            closeConsumer();
            running.set(false);
        }
    }

    public void stop() {
        running.set(false);
        Optional.ofNullable(consumer).ifPresent(KafkaConsumer::wakeup);
    }

    private KafkaConsumer<String, HubEventAvro> createConsumer() {
        AnalyzerProperties.Consumer consumerProps = properties.getKafka().getHubEvents();
        return consumerFactory.create(HubEventDeserializer.class, consumerProps, "telemetry-analyzer-hub-events");
    }

    private void processRecords(ConsumerRecords<String, HubEventAvro> records) {
        for (ConsumerRecord<String, HubEventAvro> record : records) {
            HubEventAvro event = record.value();
            if (event == null || event.getPayload() == null) {
                continue;
            }
            try {
                log.debug("event=hub-event-received hubId={} key={} partition={} offset={}",
                        event.getHubId(), record.key(), record.partition(), record.offset());
                handleEvent(event);
            } catch (Exception e) {
                log.error("event=hub-event-handle-failed hubId={} offset={}", event.getHubId(), record.offset(), e);
            }
        }
    }

    private void handleEvent(HubEventAvro event) {
        Object payload = event.getPayload();
        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            storage.handleDeviceAdded(event.getHubId(), deviceAdded);
            log.debug("Добавлено устройство {} для хаба {}", deviceAdded.getId(), event.getHubId());
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            storage.handleDeviceRemoved(deviceRemoved);
            log.debug("Удалено устройство {} из хаба {}", deviceRemoved.getId(), event.getHubId());
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            storage.handleScenarioAdded(event.getHubId(), scenarioAdded);
            log.info("Сценарий {} обновлён для хаба {}", scenarioAdded.getName(), event.getHubId());
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            storage.handleScenarioRemoved(event.getHubId(), scenarioRemoved);
            log.info("Сценарий {} удалён для хаба {}", scenarioRemoved.getName(), event.getHubId());
        } else {
            log.warn("Получен неподдерживаемый payload {}", payload.getClass());
        }
    }

    private void closeConsumer() {
        try {
            Optional.ofNullable(consumer).ifPresent(c -> {
                try {
                    c.close(Duration.ofSeconds(5));
                    log.info("HubEventProcessor consumer закрыт");
                } catch (Exception e) {
                    log.warn("Не удалось корректно закрыть consumer обработчика событий", e);
                }
            });
        } finally {
            consumer = null;
        }
    }
}