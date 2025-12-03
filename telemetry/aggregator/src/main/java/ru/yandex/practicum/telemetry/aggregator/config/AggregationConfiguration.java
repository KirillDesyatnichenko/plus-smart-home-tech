package ru.yandex.practicum.telemetry.aggregator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.telemetry.aggregator.SensorsSnapshotAggregator;

@Configuration
public class AggregationConfiguration {

    @Bean
    public SensorsSnapshotAggregator sensorsSnapshotAggregator() {
        return new SensorsSnapshotAggregator();
    }
}