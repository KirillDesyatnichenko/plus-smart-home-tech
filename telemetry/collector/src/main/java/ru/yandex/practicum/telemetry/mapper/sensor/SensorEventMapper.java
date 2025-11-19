package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;

public interface SensorEventMapper {
    SpecificRecordBase map(SensorEvent dto);
    SensorEventType getType();
}