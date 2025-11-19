package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.model.sensor.TemperatureSensorEvent;

import java.time.Instant;

@Component
public class TemperatureSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEvent dto) {
        TemperatureSensorEvent temp = (TemperatureSensorEvent) dto;

        TemperatureSensorAvro payload = TemperatureSensorAvro.newBuilder()
                .setTemperatureC(temp.getTemperatureC())
                .setTemperatureF(temp.getTemperatureF())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(temp.getId())
                .setHubId(temp.getHubId())
                .setTimestamp(temp.getTimestamp() != null ? temp.getTimestamp() : Instant.now())
                .setType(SensorEventTypeAvro.TEMPERATURE_SENSOR_EVENT)
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}