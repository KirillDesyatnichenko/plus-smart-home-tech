package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.model.hub.ScenarioAddedEvent;

import java.time.Instant;
import java.util.stream.Collectors;

@Component
public class ScenarioAddedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        ScenarioAddedEvent event = (ScenarioAddedEvent) dto;

        var conditions = event.getConditions().stream()
                .map(c -> ScenarioConditionAvro.newBuilder()
                        .setSensorId(c.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                        .setValue(c.getValue()) // Integer, nullable
                        .build())
                .collect(Collectors.toList());

        var actions = event.getActions().stream()
                .map(a -> DeviceActionAvro.newBuilder()
                        .setSensorId(a.getSensorId())
                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                        .setValue(a.getValue()) // Integer, nullable
                        .build())
                .collect(Collectors.toList());

        Instant ts = event.getTimestamp() != null ? event.getTimestamp() : Instant.now();

        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(conditions)
                .setActions(actions)
                .setHubId(event.getHubId())
                .setTimestamp(ts)
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(ts)
                .setType(HubEventTypeAvro.SCENARIO_ADDED)
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}