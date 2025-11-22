package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.model.hub.ScenarioAddedEvent;

import java.util.List;

@Component
public class ScenarioAddedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEvent dto) {
        ScenarioAddedEvent event = (ScenarioAddedEvent) dto;

        List<ScenarioConditionAvro> conditions = event.getConditions().stream()
                .map(c -> ScenarioConditionAvro.newBuilder()
                        .setSensorId(c.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                        .setValue(c.getValue())
                        .build())
                .toList();

        List<DeviceActionAvro> actions = event.getActions().stream()
                .map(a -> DeviceActionAvro.newBuilder()
                        .setSensorId(a.getSensorId())
                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                        .setValue(a.getValue())
                        .build())
                .toList();

        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}