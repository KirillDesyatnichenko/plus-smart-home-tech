package ru.yandex.practicum.telemetry.analyzer.storage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioStorage {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;

    @Transactional
    public void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        try {
            sensorRepository.findById(event.getId())
                    .map(existing -> {
                        existing.setHubId(hubId);
                        return existing;
                    })
                    .or(() -> java.util.Optional.of(Sensor.builder()
                            .id(event.getId())
                            .hubId(hubId)
                            .build()))
                    .ifPresent(sensorRepository::save);
            log.info("event=hub-device-added hubId={} sensorId={}", hubId, event.getId());
        } catch (Exception e) {
            log.error("event=hub-device-added-failed hubId={} sensorId={}", hubId, event.getId(), e);
        }
    }

    @Transactional
    public void handleDeviceRemoved(DeviceRemovedEventAvro event) {
        String sensorId = event.getId();
        try {
            List<Scenario> scenarios = scenarioRepository.findAll();
            for (Scenario scenario : scenarios) {
                boolean conditionRemoved = scenario.getConditions().remove(sensorId) != null;
                boolean actionRemoved = scenario.getActions().remove(sensorId) != null;
                if (conditionRemoved || actionRemoved) {
                    scenarioRepository.save(scenario);
                }
            }
            sensorRepository.findById(sensorId).ifPresent(sensorRepository::delete);
            log.info("event=hub-device-removed sensorId={}", sensorId);
        } catch (Exception e) {
            log.error("event=hub-device-remove-failed sensorId={}", sensorId, e);
        }
    }

    @Transactional
    public void handleScenarioAdded(String hubId, ScenarioAddedEventAvro scenarioAvro) {
        try {
            ensureSensorsExist(hubId, scenarioAvro);

            Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioAvro.getName())
                    .orElseGet(() -> Scenario.builder()
                            .hubId(hubId)
                            .name(scenarioAvro.getName())
                            .build());

            scenario.getConditions().clear();
            scenario.getActions().clear();

            scenario.getConditions().putAll(buildConditionsMap(scenarioAvro));
            scenario.getActions().putAll(buildActionsMap(scenarioAvro));

            scenarioRepository.save(scenario);
            log.info("event=scenario-upserted hubId={} scenario={}", hubId, scenarioAvro.getName());
        } catch (Exception e) {
            log.error("event=scenario-upsert-failed hubId={} scenario={}", hubId, scenarioAvro.getName(), e);
        }
    }

    @Transactional
    public void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro scenarioAvro) {
        scenarioRepository.findByHubIdAndName(hubId, scenarioAvro.getName())
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    log.info("event=scenario-removed hubId={} scenario={}", hubId, scenarioAvro.getName());
                });
    }

    @Transactional(readOnly = true)
    public List<Scenario> findByHubId(String hubId) {
        return scenarioRepository.findByHubId(hubId);
    }

    private void ensureSensorsExist(String hubId, ScenarioAddedEventAvro scenarioAvro) {
        Set<String> sensorIds = new HashSet<>();
        scenarioAvro.getConditions().forEach(c -> sensorIds.add(c.getSensorId()));
        scenarioAvro.getActions().forEach(a -> sensorIds.add(a.getSensorId()));

        for (String sensorId : sensorIds) {
            sensorRepository.findById(sensorId).ifPresentOrElse(sensor -> {
                if (!hubId.equals(sensor.getHubId())) {
                    sensor.setHubId(hubId);
                    sensorRepository.save(sensor);
                }
            }, () -> sensorRepository.save(Sensor.builder()
                    .id(sensorId)
                    .hubId(hubId)
                    .build()));
        }
    }

    private Map<String, Condition> buildConditionsMap(ScenarioAddedEventAvro scenarioAvro) {
        Map<String, Condition> result = new HashMap<>();
        for (ScenarioConditionAvro conditionAvro : scenarioAvro.getConditions()) {
            Condition condition = Condition.builder()
                    .type(ConditionType.fromAvro(conditionAvro.getType()))
                    .operation(ConditionOperation.fromAvro(conditionAvro.getOperation()))
                    .value(resolveConditionValue(conditionAvro))
                    .build();
            Condition saved = conditionRepository.save(condition);
            result.put(conditionAvro.getSensorId(), saved);
        }
        return result;
    }

    private Map<String, Action> buildActionsMap(ScenarioAddedEventAvro scenarioAvro) {
        Map<String, Action> result = new HashMap<>();
        for (DeviceActionAvro actionAvro : scenarioAvro.getActions()) {
            Action action = Action.builder()
                    .type(ActionType.fromAvro(actionAvro.getType()))
                    .value(actionAvro.getValue())
                    .build();
            Action saved = actionRepository.save(action);
            result.put(actionAvro.getSensorId(), saved);
        }
        return result;
    }

    private static Integer resolveConditionValue(ScenarioConditionAvro condition) {
        Object value = condition.getValue();
        if (value == null) {
            return null;
        }
        if (value instanceof Integer intValue) {
            return intValue;
        }
        if (value instanceof Boolean boolValue) {
            return boolValue ? 1 : 0;
        }
        throw new IllegalArgumentException("Неподдерживаемый тип значения условия: " + value.getClass());
    }
}