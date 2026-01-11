package ru.yandex.practicum.commerce.exception;

public class ProductInShoppingCartNotInWarehouse extends RuntimeException {
    public ProductInShoppingCartNotInWarehouse(String message) {
        super(message);
    }

    public ProductInShoppingCartNotInWarehouse() {
        super("Товар из корзины отсутствует в БД склада");
    }
}