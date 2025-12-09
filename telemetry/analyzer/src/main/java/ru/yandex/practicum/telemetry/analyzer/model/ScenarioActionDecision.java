package ru.yandex.practicum.telemetry.analyzer.model;

public record ScenarioActionDecision(
        String scenarioName,
        String sensorId,
        ActionType actionType,
        Integer value
) {

    public static ScenarioActionDecision of(String scenarioName, String sensorId, Action action) {
        return new ScenarioActionDecision(
                scenarioName,
                sensorId,
                action.getType(),
                action.getValue()
        );
    }
}