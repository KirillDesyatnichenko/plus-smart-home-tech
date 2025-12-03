package ru.yandex.practicum.telemetry.mapper;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public final class ProtoTimestampMapper {

    private ProtoTimestampMapper() {
        throw new AssertionError("Утилитарный класс не должен быть инициализирован");
    }

    public static Instant toInstant(Timestamp timestamp) {
        if (timestamp == null) {
            return Instant.now();
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}