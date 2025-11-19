package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventTypeAvro;
import ru.yandex.practicum.telemetry.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;

import java.time.Instant;

@Component
public class ClimateSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEvent dto) {
        ClimateSensorEvent cl = (ClimateSensorEvent) dto;

        ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                .setTemperatureC(cl.getTemperatureC())
                .setHumidity(cl.getHumidity())
                .setCo2Level(cl.getCo2Level())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(cl.getId())
                .setHubId(cl.getHubId())
                .setTimestamp(cl.getTimestamp() != null ? cl.getTimestamp() : Instant.now())
                .setType(SensorEventTypeAvro.CLIMATE_SENSOR_EVENT)
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}