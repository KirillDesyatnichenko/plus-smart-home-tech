package ru.yandex.practicum.commerce.exception;

public class NoDeliveryFoundException extends RuntimeException {
    public NoDeliveryFoundException() {
        super("Не найдена доставка");
    }
}