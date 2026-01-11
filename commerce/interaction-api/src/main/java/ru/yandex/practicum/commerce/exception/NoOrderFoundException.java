package ru.yandex.practicum.commerce.exception;

public class NoOrderFoundException extends RuntimeException {
    public NoOrderFoundException() {
        super("Не найден заказ");
    }

    public NoOrderFoundException(String message) {
        super(message);
    }
}