package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioActionDecision;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ScenarioEvaluator {

    private final Map<ConditionType, Function<SensorStateAvro, OptionalInt>> valueExtractors = new EnumMap<>(ConditionType.class);

    public ScenarioEvaluator() {
        valueExtractors.put(ConditionType.MOTION, state -> extractBoolean(state, MotionSensorAvro.class, MotionSensorAvro::getMotion));
        valueExtractors.put(ConditionType.LUMINOSITY, state -> extractInt(state, LightSensorAvro.class, LightSensorAvro::getLuminosity));
        valueExtractors.put(ConditionType.SWITCH, state -> extractBoolean(state, SwitchSensorAvro.class, SwitchSensorAvro::getState));
        valueExtractors.put(ConditionType.TEMPERATURE, this::extractTemperature);
        valueExtractors.put(ConditionType.CO2LEVEL, state -> extractInt(state, ClimateSensorAvro.class, ClimateSensorAvro::getCo2Level));
        valueExtractors.put(ConditionType.HUMIDITY, state -> extractInt(state, ClimateSensorAvro.class, ClimateSensorAvro::getHumidity));
    }

    public List<ScenarioActionDecision> evaluate(SensorsSnapshotAvro snapshot, List<Scenario> scenarios) {
        if (snapshot == null || scenarios.isEmpty()) {
            return List.of();
        }
        Map<String, SensorStateAvro> states = snapshot.getSensorsState() != null
                ? snapshot.getSensorsState()
                : Map.of();

        return scenarios.stream()
                .filter(scenario -> matchesScenario(scenario, states))
                .peek(scenario -> log.debug("Сценарий {} выполнен для хаба {}", scenario.getName(), scenario.getHubId()))
                .flatMap(scenario -> scenario.getActions().entrySet().stream()
                        .map(entry -> ScenarioActionDecision.of(
                                scenario.getName(),
                                entry.getKey(),
                                entry.getValue()
                        )))
                .collect(Collectors.toList());
    }

    private boolean matchesScenario(Scenario scenario, Map<String, SensorStateAvro> states) {
        return scenario.getConditions().entrySet().stream()
                .allMatch(entry -> matchesCondition(entry.getKey(), entry.getValue(), states));
    }

    private boolean matchesCondition(String sensorId, Condition condition, Map<String, SensorStateAvro> states) {
        if (condition.getValue() == null) {
            log.debug("Условие без опорного значения пропущено: {}", condition);
            return false;
        }
        SensorStateAvro state = states.get(sensorId);
        if (state == null || state.getData() == null) {
            log.debug("Нет данных для датчика {} в условии {}", sensorId, condition);
            return false;
        }
        Function<SensorStateAvro, OptionalInt> extractor = valueExtractors.get(condition.getType());
        if (extractor == null) {
            log.warn("Отсутствует обработчик типа условия {}", condition.getType());
            return false;
        }

        OptionalInt actual = extractor.apply(state);
        if (actual.isEmpty()) {
            log.debug("Не удалось извлечь значение по условию {}", condition);
            return false;
        }
        return compare(actual.getAsInt(), condition.getValue(), condition.getOperation());
    }

    private OptionalInt extractTemperature(SensorStateAvro state) {
        OptionalInt fromClimate = extractInt(state, ClimateSensorAvro.class, ClimateSensorAvro::getTemperatureC);
        if (fromClimate.isPresent()) {
            return fromClimate;
        }
        return extractInt(state, TemperatureSensorAvro.class, TemperatureSensorAvro::getTemperatureC);
    }

    private static <T> OptionalInt extractBoolean(SensorStateAvro state,
                                                  Class<T> expectedClass,
                                                  Function<T, Boolean> extractor) {
        Object payload = state.getData();
        if (expectedClass.isInstance(payload)) {
            Boolean value = extractor.apply(expectedClass.cast(payload));
            return OptionalInt.of(Boolean.TRUE.equals(value) ? 1 : 0);
        }
        return OptionalInt.empty();
    }

    private static <T> OptionalInt extractInt(SensorStateAvro state,
                                              Class<T> clazz,
                                              ToIntFunction<T> extractor) {
        Object payload = state.getData();
        if (clazz.isInstance(payload)) {
            return OptionalInt.of(extractor.applyAsInt(clazz.cast(payload)));
        }
        return OptionalInt.empty();
    }

    private boolean compare(int actual, int expected, ConditionOperation operation) {
        return switch (operation) {
            case EQUALS -> actual == expected;
            case GREATER_THAN -> actual > expected;
            case LOWER_THAN -> actual < expected;
        };
    }

    @FunctionalInterface
    private interface ToIntFunction<T> {
        int applyAsInt(T source);
    }
}