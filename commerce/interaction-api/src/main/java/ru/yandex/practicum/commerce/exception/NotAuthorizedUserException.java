package ru.yandex.practicum.commerce.exception;

public class NotAuthorizedUserException extends RuntimeException {
    public NotAuthorizedUserException() {
        super("Имя пользователя обязательно");
    }
}

