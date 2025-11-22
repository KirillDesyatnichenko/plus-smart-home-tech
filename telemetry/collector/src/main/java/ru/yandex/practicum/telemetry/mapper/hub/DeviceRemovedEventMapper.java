package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;

@Component
public class DeviceRemovedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        DeviceRemovedEvent event = (DeviceRemovedEvent) dto;

        DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED;
    }
}