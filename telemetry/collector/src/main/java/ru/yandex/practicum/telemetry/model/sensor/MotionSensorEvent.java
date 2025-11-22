package ru.yandex.practicum.telemetry.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class MotionSensorEvent extends SensorEvent {
    @NotNull(message = "linkQuality не может быть равна 0")
    private Integer linkQuality;

    @NotNull(message = "Движение не может быть равно 0")
    private Boolean motion;

    @NotNull(message = "Напряжение не может быть равно 0")
    private Integer voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}