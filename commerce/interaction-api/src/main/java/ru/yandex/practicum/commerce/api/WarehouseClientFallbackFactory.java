package ru.yandex.practicum.commerce.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.exception.UpstreamServiceException;

@Slf4j
@Component
public class WarehouseClientFallbackFactory implements FallbackFactory<WarehouseClient> {

    @Override
    public WarehouseClient create(Throwable cause) {
        return new WarehouseClient() {
            @Override
            public ResponseEntity<Void> newProductInWarehouse(NewProductInWarehouseRequest request) {
                log.error("Fallback: сервис склада недоступен при добавлении нового товара", cause);
                throw new UpstreamServiceException("Сервис склада временно недоступен", cause);
            }

            @Override
            public ResponseEntity<Void> addProductToWarehouse(AddProductToWarehouseRequest request) {
                log.error("Fallback: сервис склада недоступен при увеличении остатков", cause);
                throw new UpstreamServiceException("Сервис склада временно недоступен", cause);
            }

            @Override
            public ResponseEntity<BookedProductsDto> checkProductQuantityEnoughForShoppingCart(ShoppingCartDto cartDto) {
                log.error("Fallback: сервис склада недоступен при проверке доступности товаров", cause);
                throw new UpstreamServiceException("Сервис склада временно недоступен", cause);
            }

            @Override
            public ResponseEntity<AddressDto> getWarehouseAddress() {
                log.error("Fallback: сервис склада недоступен при получении адреса", cause);
                throw new UpstreamServiceException("Сервис склада временно недоступен", cause);
            }
        };
    }
}