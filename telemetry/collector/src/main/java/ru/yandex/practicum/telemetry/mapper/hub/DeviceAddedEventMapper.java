package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventTypeAvro;
import ru.yandex.practicum.telemetry.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;

import java.time.Instant;

@Component
public class DeviceAddedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        DeviceAddedEvent add = (DeviceAddedEvent) dto;

        DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                .setId(add.getId())
                .setDeviceType(DeviceTypeAvro.valueOf(add.getDeviceType().name()))
                .setHubId(add.getHubId())
                .setTimestamp(add.getTimestamp() != null ? add.getTimestamp() : Instant.now())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(add.getHubId())
                .setTimestamp(add.getTimestamp() != null ? add.getTimestamp() : Instant.now())
                .setType(HubEventTypeAvro.DEVICE_ADDED)
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}