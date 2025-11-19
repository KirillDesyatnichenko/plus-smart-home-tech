package ru.yandex.practicum.telemetry.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.service.TelemetryService;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class TelemetryController {

    private final TelemetryService service;

    @PostMapping("/sensors")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void sensors(@Valid @RequestBody SensorEvent event) {
        log.info("json sens: {}", event.toString());
        service.processSensor(event);
    }

    @PostMapping("/hubs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void hubs(@Valid @RequestBody HubEvent event) {
        log.info("json hub: {}", event.toString());
        service.processHub(event);
    }
}