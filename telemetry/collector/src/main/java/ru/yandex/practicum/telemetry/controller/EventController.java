package ru.yandex.practicum.telemetry.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.service.TelemetryService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final TelemetryService telemetryService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        handle(() -> telemetryService.processSensor(request), responseObserver);
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        handle(() -> telemetryService.processHub(request), responseObserver);
    }

    private void handle(Runnable action, StreamObserver<Empty> observer) {
        try {
            action.run();
            observer.onNext(Empty.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            log.error("Failed to process telemetry event", e);
            observer.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }
}

