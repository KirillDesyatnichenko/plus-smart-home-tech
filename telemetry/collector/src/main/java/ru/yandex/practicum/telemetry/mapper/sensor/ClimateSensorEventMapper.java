package ru.yandex.practicum.telemetry.mapper.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.mapper.ProtoTimestampMapper;

@Component
public class ClimateSensorEventMapper implements SensorEventMapper {

    @Override
    public SpecificRecordBase map(SensorEventProto event) {
        ClimateSensorProto climate = event.getClimateSensor();

        ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                .setTemperatureC(climate.getTemperatureC())
                .setHumidity(climate.getHumidity())
                .setCo2Level(climate.getCo2Level())
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
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }
}