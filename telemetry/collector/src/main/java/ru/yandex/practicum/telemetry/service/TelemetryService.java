package ru.yandex.practicum.telemetry.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
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

    private final Map<SensorEventProto.PayloadCase, SensorEventMapper> sensorMappers;
    private final Map<HubEventProto.PayloadCase, HubEventMapper> hubMappers;
    private final KafkaSender kafkaSender;

    private final String sensorsTopic;
    private final String hubsTopic;

    public TelemetryService(List<SensorEventMapper> sensorMappers,
                            List<HubEventMapper> hubMappers,
                            KafkaSender kafkaSender,
                            @Value("${kafka.topic.sensors}") String sensorsTopic,
                            @Value("${kafka.topic.hubs}") String hubsTopic) {

        this.sensorMappers = sensorMappers.stream()
                .collect(Collectors.toMap(SensorEventMapper::getPayloadCase, Function.identity()));

        this.hubMappers = hubMappers.stream()
                .collect(Collectors.toMap(HubEventMapper::getPayloadCase, Function.identity()));

        this.kafkaSender = kafkaSender;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void processSensor(SensorEventProto event) {
        SensorEventProto.PayloadCase payloadCase = event.getPayloadCase();
        if (payloadCase == SensorEventProto.PayloadCase.PAYLOAD_NOT_SET) {
            log.warn("Получено событие датчика без payload, id={}", event.getId());
            return;
        }

        SensorEventMapper mapper = sensorMappers.get(payloadCase);
        if (mapper == null) {
            log.warn("Неподдерживаемый тип события датчика {}", payloadCase);
            return;
        }
        kafkaSender.send(sensorsTopic, mapper.map(event));
    }

    public void processHub(HubEventProto event) {
        HubEventProto.PayloadCase payloadCase = event.getPayloadCase();
        if (payloadCase == HubEventProto.PayloadCase.PAYLOAD_NOT_SET) {
            log.warn("Получено событие хаба без payload, hubId={}", event.getHubId());
            return;
        }

        HubEventMapper mapper = hubMappers.get(payloadCase);
        if (mapper == null) {
            log.warn("Неподдерживаемый тип события хаба {}", payloadCase);
            return;
        }
        kafkaSender.send(hubsTopic, mapper.map(event));
    }
}