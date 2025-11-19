package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.model.sensor.SwitchSensorEvent;

import java.time.Instant;

@Component
public class SwitchSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEvent dto) {
        SwitchSensorEvent sw = (SwitchSensorEvent) dto;

        SwitchSensorAvro payload = SwitchSensorAvro.newBuilder()
                .setState(sw.getState())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(sw.getId())
                .setHubId(sw.getHubId())
                .setTimestamp(sw.getTimestamp() != null ? sw.getTimestamp() : Instant.now())
                .setType(SensorEventTypeAvro.SWITCH_SENSOR_EVENT)
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }
}