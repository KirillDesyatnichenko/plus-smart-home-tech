package ru.yandex.practicum.commerce.exception;

import lombok.Getter;

import java.util.UUID;

@Getter
public class ProductNotFoundException extends RuntimeException {
    private final UUID productId;
    private final String userMessage;

    public ProductNotFoundException(UUID productId) {
        super("Товар не найден: id=" + productId);
        this.productId = productId;
        this.userMessage = "Товар не найден: id=" + productId;
    }
}