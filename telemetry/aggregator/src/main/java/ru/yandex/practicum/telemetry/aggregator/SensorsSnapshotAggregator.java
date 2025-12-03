package ru.yandex.practicum.telemetry.aggregator;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SensorsSnapshotAggregator {

    private final ConcurrentMap<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        if (!isEventValid(event)) {
            return Optional.empty();
        }

        final String hubId = event.getHubId();
        final String sensorId = event.getId();
        final Instant eventTimestamp = event.getTimestamp();
        final Object payload = event.getPayload();

        final SensorsSnapshotAvro[] updatedSnapshotRef = new SensorsSnapshotAvro[1];

        snapshots.compute(hubId, (id, existingSnapshot) -> {
            SensorsSnapshotAvro snapshot = existingSnapshot != null
                    ? existingSnapshot
                    : createSnapshot(hubId, eventTimestamp);

            Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
            if (sensorsState == null) {
                sensorsState = new HashMap<>();
                snapshot.setSensorsState(sensorsState);
            }

            SensorStateAvro currentState = sensorsState.get(sensorId);
            if (currentState != null) {
                Instant currentTimestamp = currentState.getTimestamp();
                if (currentTimestamp != null && eventTimestamp.isBefore(currentTimestamp)) {
                    return snapshot;
                }
                if (Objects.equals(currentState.getData(), payload)) {
                    return snapshot;
                }
            }

            SensorStateAvro newState = SensorStateAvro.newBuilder()
                    .setTimestamp(eventTimestamp)
                    .setData(payload)
                    .build();

            sensorsState.put(sensorId, newState);
            snapshot.setTimestamp(eventTimestamp);

            updatedSnapshotRef[0] = SensorsSnapshotAvro.newBuilder(snapshot).build();
            return snapshot;
        });

        return Optional.ofNullable(updatedSnapshotRef[0]);
    }

    private static boolean isEventValid(SensorEventAvro event) {
        return event != null
                && event.getHubId() != null
                && event.getId() != null
                && event.getTimestamp() != null
                && event.getPayload() != null;
    }

    private static SensorsSnapshotAvro createSnapshot(String hubId, Instant timestamp) {
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(new HashMap<>())
                .build();
    }
}