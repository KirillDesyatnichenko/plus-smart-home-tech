package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.mapper.ProtoTimestampMapper;

@Component
public class ScenarioRemovedEventMapper implements HubEventMapper {

    @Override
    public SpecificRecordBase map(HubEventProto event) {
        ScenarioRemovedEventProto payloadProto = event.getScenarioRemoved();

        ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                .setName(payloadProto.getName())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(ProtoTimestampMapper.toInstant(event.hasTimestamp() ? event.getTimestamp() : null))
                .setPayload(payload)
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getPayloadCase() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }
}