package ru.yandex.practicum.commerce.exception;

public class ProductInShoppingCartLowQuantityInWarehouse extends RuntimeException {
    public ProductInShoppingCartLowQuantityInWarehouse(String message) {
        super(message);
    }

    public ProductInShoppingCartLowQuantityInWarehouse() {
        super("Недостаточно товара на складе");
    }
}
