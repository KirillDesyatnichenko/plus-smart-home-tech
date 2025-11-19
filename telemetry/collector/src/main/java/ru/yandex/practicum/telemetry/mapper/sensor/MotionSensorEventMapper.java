package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventTypeAvro;
import ru.yandex.practicum.telemetry.model.sensor.MotionSensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.sensor.SensorEventType;

import java.time.Instant;

@Component
public class MotionSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEvent dto) {
        MotionSensorEvent motion = (MotionSensorEvent) dto;

        MotionSensorAvro payload = MotionSensorAvro.newBuilder()
                .setLinkQuality(motion.getLinkQuality())
                .setMotion(motion.getMotion())
                .setVoltage(motion.getVoltage())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(motion.getId())
                .setHubId(motion.getHubId())
                .setTimestamp(motion.getTimestamp() != null ? motion.getTimestamp() : Instant.now())
                .setType(SensorEventTypeAvro.MOTION_SENSOR_EVENT)
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}