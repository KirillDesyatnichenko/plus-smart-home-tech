package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "aggregator")
public class AggregatorProperties {

    private Kafka kafka = new Kafka();
    private long pollTimeoutMs = 1000;

    @Data
    public static class Kafka {
        private String bootstrapServers;
        private String sensorsTopic;
        private String snapshotsTopic;
        private String groupId = "telemetry-aggregator";
    }
}