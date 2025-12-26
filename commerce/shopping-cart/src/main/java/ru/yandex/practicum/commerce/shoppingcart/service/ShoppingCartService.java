package ru.yandex.practicum.commerce.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {
    private final ShoppingCartTransactionalService transactionalService;
    private final WarehouseClient warehouseClient;

    public ShoppingCartDto getShoppingCart(String username) {
        log.info("Получение корзины пользователя {}", username);
        return transactionalService.getShoppingCart(username);
    }

    public ShoppingCartDto addProductToShoppingCart(String username, Map<UUID, Long> productsToAdd) {
        log.info("Добавление товаров в корзину пользователя {}: {}", username, productsToAdd);
        ShoppingCartDto cartDto = transactionalService.addProductToShoppingCart(username, productsToAdd);
        log.info("Товары добавлены, проверяем остатки на складе. Корзина: {}", cartDto);
        validateWithWarehouse(cartDto);
        log.info("Проверка склада пройдена. Корзина: {}", cartDto);
        return cartDto;
    }

    public ShoppingCartDto removeFromShoppingCart(String username, Iterable<UUID> productIds) {
        log.info("Удаление товаров из корзины пользователя {}: {}", username, productIds);
        return transactionalService.removeFromShoppingCart(username, productIds);
    }

    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("Изменение количества товара в корзине пользователя {}: {}", username, request);
        ShoppingCartDto cartDto = transactionalService.changeProductQuantity(username, request);
        log.info("Количество изменено, проверяем остатки на складе. Корзина: {}", cartDto);
        validateWithWarehouse(cartDto);
        log.info("Проверка склада пройдена. Корзина: {}", cartDto);
        return cartDto;
    }

    public void deactivateCurrentShoppingCart(String username) {
        log.info("Деактивация корзины пользователя {}", username);
        transactionalService.deactivateCurrentShoppingCart(username);
    }

    private void validateWithWarehouse(ShoppingCartDto cartDto) {
        log.info("Валидация корзины в сервисе склада. Корзина: {}", cartDto);
        warehouseClient.checkProductQuantityEnoughForShoppingCart(cartDto);
        log.info("Валидация склада успешно завершена. Корзина: {}", cartDto);
    }
}