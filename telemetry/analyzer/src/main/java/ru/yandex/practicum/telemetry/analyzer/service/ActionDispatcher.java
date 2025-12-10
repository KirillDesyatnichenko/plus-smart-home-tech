package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioActionDecision;
import ru.yandex.practicum.telemetry.analyzer.util.GrpcTimestampMapper;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
public class ActionDispatcher {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public void dispatch(String hubId, Instant timestamp, List<ScenarioActionDecision> decisions) {
        if (decisions.isEmpty()) {
            return;
        }
        for (ScenarioActionDecision decision : decisions) {
            sendRequest(hubId, timestamp, decision);
        }
    }

    private void sendRequest(String hubId, Instant snapshotTimestamp, ScenarioActionDecision decision) {
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(decision.sensorId())
                .setType(decision.actionType().toProto());
        if (decision.value() != null) {
            actionBuilder.setValue(decision.value());
        }

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(decision.scenarioName())
                .setAction(actionBuilder.build())
                .setTimestamp(GrpcTimestampMapper.fromInstant(snapshotTimestamp))
                .build();

        try {
            hubRouterClient.handleDeviceAction(request);
            log.info("event=device-action-sent hubId={} scenario={} actionType={} sensorId={}",
                    hubId, decision.scenarioName(), decision.actionType(), decision.sensorId());
        } catch (Exception e) {
            log.error("event=device-action-send-failed hubId={} scenario={} actionType={} sensorId={}",
                    hubId, decision.scenarioName(), decision.actionType(), decision.sensorId(), e);
        }
    }
}