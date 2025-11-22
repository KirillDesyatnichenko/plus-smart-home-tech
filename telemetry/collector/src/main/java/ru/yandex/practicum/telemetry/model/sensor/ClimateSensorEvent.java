package ru.yandex.practicum.telemetry.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class ClimateSensorEvent extends SensorEvent {
    @NotNull(message = "Температура не может быть равна 0")
    private Integer temperatureC;

    @NotNull(message = "Влажность не может быть равна 0")
    private Integer humidity;

    @NotNull(message = "Уровень углекислого газа не может быть равен 0")
    private Integer co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}