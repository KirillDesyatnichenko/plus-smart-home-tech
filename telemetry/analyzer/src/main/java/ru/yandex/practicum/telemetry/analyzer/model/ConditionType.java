package ru.yandex.practicum.telemetry.analyzer.model;

import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;

@Slf4j
public enum ConditionType {
    MOTION,
    LUMINOSITY,
    SWITCH,
    TEMPERATURE,
    CO2LEVEL,
    HUMIDITY;

    public static ConditionType fromAvro(ConditionTypeAvro avro) {
        if (avro == null) {
            throw new IllegalArgumentException("Тип условия не может быть null");
        }
        return ConditionType.valueOf(avro.name());
    }

    public static ConditionType safeValueOf(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Строковое значение типа условия не может быть null");
        }
        try {
            return ConditionType.valueOf(value);
        } catch (IllegalArgumentException ex) {
            log.warn("Не удалось преобразовать значение {} к ConditionType", value, ex);
            throw ex;
        }
    }
}