package ru.yandex.practicum.commerce.warehouse.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.exception.ProductInShoppingCartNotInWarehouse;
import ru.yandex.practicum.commerce.exception.SpecifiedProductAlreadyInWarehouseException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({
            NoSpecifiedProductInWarehouseException.class,
            ProductInShoppingCartNotInWarehouse.class,
            ProductInShoppingCartLowQuantityInWarehouse.class,
            SpecifiedProductAlreadyInWarehouseException.class
    })
    public ResponseEntity<Exception> handleBadRequest(Exception ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex);
    }
}