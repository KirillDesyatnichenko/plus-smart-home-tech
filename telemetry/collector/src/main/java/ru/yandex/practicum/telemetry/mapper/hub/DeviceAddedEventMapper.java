package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.mapper.ProtoTimestampMapper;

@Component
public class DeviceAddedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEventProto event) {
        DeviceAddedEventProto payloadProto = event.getDeviceAdded();

        DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                .setId(payloadProto.getId())
                .setType(DeviceTypeAvro.valueOf(payloadProto.getType().name()))
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(ProtoTimestampMapper.toInstant(event.hasTimestamp() ? event.getTimestamp() : null))
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getPayloadCase() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }
}