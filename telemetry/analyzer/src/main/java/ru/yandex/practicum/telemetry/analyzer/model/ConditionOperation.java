package ru.yandex.practicum.telemetry.analyzer.model;

import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;

@Slf4j
public enum ConditionOperation {
    EQUALS,
    GREATER_THAN,
    LOWER_THAN;

    public static ConditionOperation fromAvro(ConditionOperationAvro avro) {
        if (avro == null) {
            throw new IllegalArgumentException("Операция сравнения не может быть null");
        }
        return ConditionOperation.valueOf(avro.name());
    }

    public static ConditionOperation safeValueOf(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Строковое значение операции не может быть null");
        }
        try {
            return ConditionOperation.valueOf(value);
        } catch (IllegalArgumentException ex) {
            log.warn("Не удалось преобразовать значение {} к ConditionOperation", value, ex);
            throw ex;
        }
    }
}