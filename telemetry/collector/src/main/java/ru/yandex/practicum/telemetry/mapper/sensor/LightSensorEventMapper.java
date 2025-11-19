package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventTypeAvro;
import ru.yandex.practicum.telemetry.model.sensor.LightSensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;

import java.time.Instant;

@Component
public class LightSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEvent dto) {
        LightSensorEvent light = (LightSensorEvent) dto;

        LightSensorAvro payload = LightSensorAvro.newBuilder()
                .setLinkQuality(light.getLinkQuality())
                .setLuminosity(light.getLuminosity())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(light.getId())
                .setHubId(light.getHubId())
                .setTimestamp(light.getTimestamp() != null ? light.getTimestamp() : Instant.now())
                .setType(SensorEventTypeAvro.LIGHT_SENSOR_EVENT)
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}