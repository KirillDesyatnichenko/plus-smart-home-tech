package ru.yandex.practicum.telemetry.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeviceAction {
    @NotBlank(message = "Id не может быть пустым")
    private String sensorId;

    @NotNull(message = "Тип действия не может быть пустым")
    private ActionType type;

    private Integer value;
}