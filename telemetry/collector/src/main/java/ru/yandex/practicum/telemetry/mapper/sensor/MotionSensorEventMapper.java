package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.mapper.ProtoTimestampMapper;

@Component
public class MotionSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEventProto event) {
        MotionSensorProto motion = event.getMotionSensor();

        MotionSensorAvro payload = MotionSensorAvro.newBuilder()
                .setLinkQuality(motion.getLinkQuality())
                .setMotion(motion.getMotion())
                .setVoltage(motion.getVoltage())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(ProtoTimestampMapper.toInstant(event.hasTimestamp() ? event.getTimestamp() : null))
                .setPayload(payload)
                .build();
    }

    @Override
    public SensorEventProto.PayloadCase getPayloadCase() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }
}