package ru.yandex.practicum.telemetry.mapper.hub;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubEventMapper {

    SpecificRecordBase map(HubEventProto event);

    HubEventProto.PayloadCase getPayloadCase();
}