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
        private String bootstrapServers;
        private Topics topics = new Topics();
        private Consumer hubEvents = new Consumer();
        private Consumer snapshots = new Consumer();
    }

    @Data
    public static class Topics {
        private String hubEvents;
        private String snapshots;
    }

    @Data
    public static class Consumer {
        private String groupId;
        private long pollTimeoutMs;
        private boolean enableAutoCommit;
        private Map<String, Object> properties = new HashMap<>();
    }
}