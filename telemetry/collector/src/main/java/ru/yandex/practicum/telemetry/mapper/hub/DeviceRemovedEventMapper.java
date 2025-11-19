package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventTypeAvro;
import ru.yandex.practicum.telemetry.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;

import java.time.Instant;

@Component
public class DeviceRemovedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        DeviceRemovedEvent removed = (DeviceRemovedEvent) dto;

        DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                .setId(removed.getId())
                .setHubId(removed.getHubId())
                .setTimestamp(removed.getTimestamp() != null ? removed.getTimestamp() : Instant.now())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(removed.getHubId())
                .setTimestamp(removed.getTimestamp() != null ? removed.getTimestamp() : Instant.now())
                .setType(HubEventTypeAvro.DEVICE_REMOVED)
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED;
    }
}