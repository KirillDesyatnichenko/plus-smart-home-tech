package ru.yandex.practicum.telemetry.analyzer.model;

import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;

@Slf4j
public enum ActionType {
    ACTIVATE,
    DEACTIVATE,
    INVERSE,
    SET_VALUE;

    public static ActionType fromAvro(ActionTypeAvro avro) {
        if (avro == null) {
            throw new IllegalArgumentException("Тип действия не может быть null");
        }
        return ActionType.valueOf(avro.name());
    }

    public static ActionType safeValueOf(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Строковое значение типа действия не может быть null");
        }
        try {
            return ActionType.valueOf(value);
        } catch (IllegalArgumentException ex) {
            log.warn("Не удалось преобразовать значение {} к ActionType", value, ex);
            throw ex;
        }
    }

    public ActionTypeProto toProto() {
        return ActionTypeProto.valueOf(name());
    }
}