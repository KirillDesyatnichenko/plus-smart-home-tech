package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.mapper.ProtoTimestampMapper;

@Component
public class ScenarioAddedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEventProto event) {
        ScenarioAddedEventProto payloadProto = event.getScenarioAdded();

        var conditions = payloadProto.getConditionList().stream()
                .map(c -> ScenarioConditionAvro.newBuilder()
                        .setSensorId(c.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                        .setValue(resolveConditionValue(c))
                        .build())
                .toList();

        var actions = payloadProto.getActionList().stream()
                .map(a -> DeviceActionAvro.newBuilder()
                        .setSensorId(a.getSensorId())
                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                        .setValue(a.hasValue() ? a.getValue() : null)
                        .build())
                .toList();

        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(payloadProto.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(ProtoTimestampMapper.toInstant(event.hasTimestamp() ? event.getTimestamp() : null))
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getPayloadCase() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    private Object resolveConditionValue(ScenarioConditionProto condition) {
        return switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
            default -> null;
        };
    }
}