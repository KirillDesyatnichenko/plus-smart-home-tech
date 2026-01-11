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
import ru.yandex.practicum.commerce.dto.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.commerce.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.commerce.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.exception.ProductInShoppingCartNotInWarehouse;
import ru.yandex.practicum.commerce.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.model.OrderBooking;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;
import ru.yandex.practicum.commerce.warehouse.repository.OrderBookingRepository;

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
    private final OrderBookingRepository orderBookingRepository;
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
                    .orElseThrow(() -> new ProductInShoppingCartNotInWarehouse(
                            "Товар не найден на складе: " + productId));

            if (product.getQuantity() < required) {
                log.warn("Недостаточно товара на складе: productId={}, нужно={}, доступно={}",
                        productId, required, product.getQuantity());
                throw new ProductInShoppingCartNotInWarehouse(
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

    @Transactional
    public BookedProductsDto assemblyProductsForOrder(AssemblyProductsForOrderRequest request) {
        log.info("Сборка товаров для заказа {}", request.getOrderId());
        BookedProductsDto booked = checkAndCalculate(request.getProducts());

        request.getProducts().forEach((productId, qty) -> {
            WarehouseProduct product = repository.findByProductId(productId)
                    .orElseThrow(NoSpecifiedProductInWarehouseException::new);
            product.setQuantity(product.getQuantity() - qty);
            repository.save(product);
        });

        OrderBooking booking = orderBookingRepository.findByOrderId(request.getOrderId())
                .orElse(OrderBooking.builder().orderId(request.getOrderId()).build());
        booking.setProducts(request.getProducts());
        orderBookingRepository.save(booking);

        return booked;
    }

    @Transactional
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        log.info("Передача заказа {} в доставку {}", request.getOrderId(), request.getDeliveryId());
        OrderBooking booking = orderBookingRepository.findByOrderId(request.getOrderId())
                .orElseThrow(NoSpecifiedProductInWarehouseException::new);
        booking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(booking);
    }

    @Transactional
    public void acceptReturn(Map<UUID, Long> products) {
        log.info("Принят возврат товаров: {}", products);
        products.forEach((productId, qty) -> {
            WarehouseProduct product = repository.findByProductId(productId)
                    .orElseThrow(NoSpecifiedProductInWarehouseException::new);
            product.setQuantity(product.getQuantity() + qty);
            repository.save(product);
        });
    }

    private BookedProductsDto checkAndCalculate(Map<UUID, Long> products) {
        double totalWeight = 0d;
        double totalVolume = 0d;
        boolean fragile = false;
        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            long required = entry.getValue();
            WarehouseProduct product = repository.findByProductId(productId)
                    .orElseThrow(() -> new ProductInShoppingCartLowQuantityInWarehouse(
                            "Товар не найден на складе: " + productId));
            if (product.getQuantity() < required) {
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
        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(fragile)
                .build();
    }
}