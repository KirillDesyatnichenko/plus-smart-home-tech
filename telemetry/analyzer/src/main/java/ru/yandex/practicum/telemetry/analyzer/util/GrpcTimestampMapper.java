package ru.yandex.practicum.telemetry.analyzer.util;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public final class GrpcTimestampMapper {

    private GrpcTimestampMapper() {
    }

    public static Timestamp fromInstant(Instant instant) {
        Instant value = instant != null ? instant : Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(value.getEpochSecond())
                .setNanos(value.getNano())
                .build();
    }
}