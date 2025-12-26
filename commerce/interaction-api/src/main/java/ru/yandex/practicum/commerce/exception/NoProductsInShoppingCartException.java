package ru.yandex.practicum.commerce.exception;

public class NoProductsInShoppingCartException extends RuntimeException {
    public NoProductsInShoppingCartException() {
        super("Указанные товары отсутствуют в корзине");
    }
}