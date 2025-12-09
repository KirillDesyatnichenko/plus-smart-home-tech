package ru.yandex.practicum.telemetry.analyzer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "analyzer")
public class AnalyzerProperties {

    private Kafka kafka = new Kafka();

    @Data
    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private Topics topics = new Topics();
        private Consumer hubEvents = Consumer.defaults(true, 500);
        private Consumer snapshots = Consumer.defaults(false, 200);
    }

    @Data
    public static class Topics {
        private String hubEvents = "telemetry.hubs.v1";
        private String snapshots = "telemetry.snapshots.v1";
    }

    @Data
    public static class Consumer {
        private String groupId;
        private long pollTimeoutMs = 200;
        private boolean enableAutoCommit = false;
        private Map<String, Object> properties = new HashMap<>();

        public static Consumer defaults(boolean enableAutoCommit, long pollTimeoutMs) {
            Consumer consumer = new Consumer();
            consumer.setEnableAutoCommit(enableAutoCommit);
            consumer.setPollTimeoutMs(pollTimeoutMs);
            return consumer;
        }
    }
}