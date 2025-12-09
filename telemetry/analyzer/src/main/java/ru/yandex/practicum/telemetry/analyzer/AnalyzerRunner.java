package ru.yandex.practicum.telemetry.analyzer;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.processor.SnapshotProcessor;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner implements CommandLineRunner {

    private final HubEventProcessor hubEventProcessor;
    private final SnapshotProcessor snapshotProcessor;
    private Thread hubEventsThread;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    @Override
    public void run(String... args) {
        hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        log.info("Analyzer started: hubEventsThread={}, snapshotProcessor=main-thread", hubEventsThread.getName());
        snapshotProcessor.start();
    }

    @PreDestroy
    public void shutdown() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        log.info("Получен сигнал на остановку анализатора");
        try {
            snapshotProcessor.stop();
        } catch (Exception e) {
            log.warn("Не удалось корректно остановить обработчик снапшотов", e);
        }
        try {
            hubEventProcessor.stop();
        } catch (Exception e) {
            log.warn("Не удалось корректно остановить обработчик событий хаба", e);
        }
        if (hubEventsThread != null) {
            try {
                hubEventsThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Ожидание завершения потока событий хаба прервано", e);
            }
        }
    }
}