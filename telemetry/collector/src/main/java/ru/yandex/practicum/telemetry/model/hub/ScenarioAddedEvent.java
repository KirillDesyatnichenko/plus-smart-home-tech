package ru.yandex.practicum.telemetry.model.hub;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioAddedEvent extends HubEvent {
    @NotNull(message = "Имя не может быть пустым")
    @Size(min = 3)
    private String name;

    @NotEmpty(message = "List conditions не может быть пустым")
    private List<ScenarioCondition> conditions;

    @NotEmpty(message = "List actions не может быть пустым")
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}