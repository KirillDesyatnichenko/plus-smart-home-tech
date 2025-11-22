package ru.yandex.practicum.telemetry.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioCondition {
    @NotBlank(message = "Id не может быть пустым")
    private String sensorId;

    @NotNull(message = "ConditionType не может быть пустым")
    private ConditionType type;

    @NotNull(message = "ConditionOperation не может быть пустым")
    private ConditionOperation operation;

    @NotNull(message = "Value не может быть пустым")
    private Integer value;
}