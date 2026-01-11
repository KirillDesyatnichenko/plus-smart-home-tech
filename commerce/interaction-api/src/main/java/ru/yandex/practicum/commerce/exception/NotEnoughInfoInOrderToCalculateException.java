package ru.yandex.practicum.commerce.exception;

public class NotEnoughInfoInOrderToCalculateException extends RuntimeException {
    public NotEnoughInfoInOrderToCalculateException() {
        super("Недостаточно информации в заказе для расчёта");
    }
}