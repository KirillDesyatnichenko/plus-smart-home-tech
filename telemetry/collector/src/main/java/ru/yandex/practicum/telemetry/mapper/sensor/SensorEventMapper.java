package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventMapper {

    SpecificRecordBase map(SensorEventProto event);

    SensorEventProto.PayloadCase getPayloadCase();
}