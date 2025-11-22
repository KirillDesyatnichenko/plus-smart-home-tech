package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.model.hub.ScenarioRemovedEvent;

@Component
public class ScenarioRemovedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        ScenarioRemovedEvent event = (ScenarioRemovedEvent) dto;

        ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}