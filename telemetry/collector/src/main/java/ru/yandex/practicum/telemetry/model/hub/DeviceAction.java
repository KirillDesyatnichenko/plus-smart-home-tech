package ru.yandex.practicum.telemetry.model.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeviceAction {
    @NotBlank
    @JsonProperty("sensor_id")
    private String sensorId;

    @NotNull
    private ActionType type;

    private Integer value;
}