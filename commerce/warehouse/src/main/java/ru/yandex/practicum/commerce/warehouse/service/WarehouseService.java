package ru.yandex.practicum.commerce.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseService {
    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS =
            ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    private final WarehouseProductRepository repository;
    private final WarehouseMapper mapper;

    @Transactional
    public void newProductInWarehouse(NewProductInWarehouseRequest request) {
        log.info("Добавление нового товара на склад: {}", request);
        repository.findByProductId(request.getProductId())
                .ifPresent(existing -> {
                    throw new SpecifiedProductAlreadyInWarehouseException();
                });
        WarehouseProduct product = mapper.toEntity(request);
        repository.save(product);
        log.info("Товар добавлен на склад: productId={}", request.getProductId());
    }

    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        log.info("Увеличение остатков на складе: {}", request);
        WarehouseProduct product = repository.findByProductId(request.getProductId())
                .orElseThrow(NoSpecifiedProductInWarehouseException::new);
        product.setQuantity(product.getQuantity() + request.getQuantity());
        repository.save(product);
        log.info("Остатки обновлены: productId={}, новое количество={}",
                request.getProductId(), product.getQuantity());
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto cart) {
        log.info("Проверка наличия товаров для корзины: {}", cart);
        double totalWeight = 0d;
        double totalVolume = 0d;
        boolean fragile = false;

        for (Map.Entry<UUID, Long> entry : cart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            long required = entry.getValue();

            WarehouseProduct product = repository.findByProductId(productId)
                    .orElseThrow(() -> new ProductInShoppingCartLowQuantityInWarehouse(
                            "Товар не найден на складе: " + productId));

            if (product.getQuantity() < required) {
                log.warn("Недостаточно товара на складе: productId={}, нужно={}, доступно={}",
                        productId, required, product.getQuantity());
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "Недостаточно товара на складе: " + productId);
            }

            totalWeight += product.getWeight() * required;
            double volume = product.getDimension().getWidth()
                    * product.getDimension().getHeight()
                    * product.getDimension().getDepth();
            totalVolume += volume * required;
            fragile = fragile || Boolean.TRUE.equals(product.getFragile());
        }

        BookedProductsDto result = BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(fragile)
                .build();
        log.info("Проверка наличия завершена: {}", result);
        return result;
    }

    @Transactional(readOnly = true)
    public AddressDto getWarehouseAddress() {
        log.info("Получение адреса склада");
        AddressDto result = mapper.toAddressDto(CURRENT_ADDRESS);
        log.info("Адрес склада: {}", result);
        return result;
    }
}