package ru.yandex.practicum.commerce.exception;

public class SpecifiedProductAlreadyInWarehouseException extends RuntimeException {
    public SpecifiedProductAlreadyInWarehouseException() {
        super("Товар уже зарегистрирован на складе");
    }
}
