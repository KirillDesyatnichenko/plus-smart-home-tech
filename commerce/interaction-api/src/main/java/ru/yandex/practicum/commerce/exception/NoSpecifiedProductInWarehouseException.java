package ru.yandex.practicum.commerce.exception;

public class NoSpecifiedProductInWarehouseException extends RuntimeException {
    public NoSpecifiedProductInWarehouseException() {
        super("На складе нет указанного товара");
    }
}