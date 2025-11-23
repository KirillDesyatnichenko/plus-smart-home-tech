package ru.yandex.practicum.telemetry.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.model.sensor.*;
import ru.yandex.practicum.telemetry.model.hub.*;
import ru.yandex.practicum.telemetry.kafka.KafkaSender;
import ru.yandex.practicum.telemetry.mapper.hub.HubEventMapper;
import ru.yandex.practicum.telemetry.mapper.sensor.SensorEventMapper;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TelemetryService {

    private final Map<SensorEventType, SensorEventMapper> sensorMappers;
    private final Map<HubEventType, HubEventMapper> hubMappers;
    private final KafkaSender kafkaSender;

    private final String sensorsTopic;
    private final String hubsTopic;

    public TelemetryService(List<SensorEventMapper> sensorMappers,
                            List<HubEventMapper> hubMappers,
                            KafkaSender kafkaSender,
                            @Value("${kafka.topic.sensors}") String sensorsTopic,
                            @Value("${kafka.topic.hubs}") String hubsTopic) {

        this.sensorMappers = sensorMappers.stream()
                .collect(Collectors.toMap(SensorEventMapper::getType, Function.identity()));

        this.hubMappers = hubMappers.stream()
                .collect(Collectors.toMap(HubEventMapper::getType, Function.identity()));

        this.kafkaSender = kafkaSender;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void processSensor(SensorEvent dto) {
        SensorEventMapper mapper = sensorMappers.get(dto.getType());
        if (mapper == null) {
            log.warn("Неподдерживаемый тип события датчика {}", dto.getType());
            return;
        }
        kafkaSender.send(sensorsTopic, mapper.map(dto));
    }

    public void processHub(HubEvent dto) {
        HubEventMapper mapper = hubMappers.get(dto.getType());
        if (mapper == null) {
            log.warn("Неподдерживаемый тип события хаба {}", dto.getType());
            return;
        }
        kafkaSender.send(hubsTopic, mapper.map(dto));
    }
}