package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import ru.yandex.practicum.commerce.api.DeliveryClient;
import ru.yandex.practicum.commerce.api.PaymentClient;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.dto.DeliveryState;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.ProductReturnRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.order.model.OrderState;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final double DELIVERY_WEIGHT_PRICE_MULTIPLIER = 10.0;
    private static final double DELIVERY_VOLUME_PRICE_MULTIPLIER = 5.0;

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final WarehouseClient warehouseClient;
    private final ShoppingStoreClient shoppingStoreClient;
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;

    @Transactional(readOnly = true)
    public List<OrderDto> getClientOrders(String username) {
        validateUsername(username);
        log.info("Получение заказов пользователя {}", username);
        return orderRepository.findAllByUsername(username)
                .stream()
                .map(orderMapper::toDto)
                .toList();
    }

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        log.info("Создание нового заказа: {}", request);
        if (request == null || request.getShoppingCart() == null) {
            throw new NoSpecifiedProductInWarehouseException();
        }

        ShoppingCartDto cartDto = request.getShoppingCart();
        if (cartDto.getProducts() == null || cartDto.getProducts().isEmpty()) {
            throw new NoSpecifiedProductInWarehouseException();
        }

        String username = request.getUsername();
        validateUsername(username);

        BookedProductsDto booked = warehouseClient.checkProductQuantityEnoughForShoppingCart(cartDto).getBody();

        Order order = Order.builder()
                .shoppingCartId(cartDto.getShoppingCartId())
                .products(cartDto.getProducts())
                .state(OrderState.NEW)
                .username(username)
                .deliveryWeight(booked != null ? booked.getDeliveryWeight() : null)
                .deliveryVolume(booked != null ? booked.getDeliveryVolume() : null)
                .fragile(booked != null ? booked.getFragile() : null)
                .productPrice(calculateProductsPrice(cartDto.getProducts()))
                .build();

        Order saved = orderRepository.save(order);
        planDelivery(saved, request.getDeliveryAddress());
        log.info("Заказ создан: {}", saved.getOrderId());
        return orderMapper.toDto(saved);
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest productReturnRequest) {
        log.info("Возврат товаров по заказу: {}", productReturnRequest);
        if (productReturnRequest == null || productReturnRequest.getOrderId() == null) {
            throw new NoOrderFoundException();
        }
        Order order = getOrderOrThrow(productReturnRequest.getOrderId());
        order.setState(OrderState.PRODUCT_RETURNED);
        Order saved = orderRepository.save(order);
        log.info("Статус заказа {} обновлён на PRODUCT_RETURNED", order.getOrderId());
        return orderMapper.toDto(saved);
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.PAID);
        if (order.getPaymentId() == null) {
            order.setPaymentId(UUID.randomUUID());
        }
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Оплата заказа {} подтверждена", orderId);
        return result;
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Оплата заказа {} завершилась ошибкой", orderId);
        return result;
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        if (order.getDeliveryId() == null) {
            order.setDeliveryId(UUID.randomUUID());
        }
        order.setState(OrderState.DELIVERED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Доставка заказа {} подтверждена", orderId);
        return result;
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Доставка заказа {} завершилась ошибкой", orderId);
        return result;
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.COMPLETED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Заказ {} завершён", orderId);
        return result;
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        if (order.getProductPrice() == null) {
            order.setProductPrice(paymentClient.productCost(orderMapper.toDto(order)).getBody());
        }
        if (order.getDeliveryPrice() == null) {
            order.setDeliveryPrice(deliveryClient.deliveryCost(orderMapper.toDto(order)).getBody());
        }
        order.setTotalPrice(paymentClient.getTotalCost(orderMapper.toDto(order)).getBody());
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Для заказа {} рассчитана полная стоимость: {}", orderId, result.getTotalPrice());
        return result;
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setDeliveryPrice(deliveryClient.deliveryCost(orderMapper.toDto(order)).getBody());
        order.setState(OrderState.ON_DELIVERY);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Для заказа {} рассчитана стоимость доставки: {}", orderId, result.getDeliveryPrice());
        return result;
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.ASSEMBLED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Заказ {} собран", orderId);
        return result;
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        OrderDto result = orderMapper.toDto(orderRepository.save(order));
        log.info("Сборка заказа {} завершилась ошибкой", orderId);
        return result;
    }

    @Transactional
    private Order getOrderOrThrow(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(NoOrderFoundException::new);
    }

    private void validateUsername(String username) {
        if (!StringUtils.hasText(username)) {
            throw new NotAuthorizedUserException();
        }
    }

    private double calculateProductsPrice(Map<UUID, Long> products) {
        if (products == null || products.isEmpty()) {
            return 0d;
        }

        return products.entrySet().stream()
                .mapToDouble(entry -> {
                    var response = shoppingStoreClient.getProduct(entry.getKey());
                    var product = response.getBody();
                    if (product == null || product.getPrice() == null) {
                        return 0d;
                    }
                    return product.getPrice() * entry.getValue();
                })
                .sum();
    }

    private double calculateDeliveryPrice(Order order) {
        double weight = safeValue(order.getDeliveryWeight());
        double volume = safeValue(order.getDeliveryVolume());
        return weight * DELIVERY_WEIGHT_PRICE_MULTIPLIER + volume * DELIVERY_VOLUME_PRICE_MULTIPLIER;
    }

    private double safeValue(Double value) {
        return value == null ? 0d : value;
    }

    private void planDelivery(Order order, AddressDto toAddress) {
        AddressDto fromAddress = warehouseClient.getWarehouseAddress().getBody();
        DeliveryDto deliveryDto = DeliveryDto.builder()
                .deliveryId(UUID.randomUUID())
                .orderId(order.getOrderId())
                .fromAddress(fromAddress)
                .toAddress(toAddress)
                .deliveryState(DeliveryState.CREATED)
                .deliveryWeight(order.getDeliveryWeight())
                .deliveryVolume(order.getDeliveryVolume())
                .fragile(order.getFragile())
                .build();
        DeliveryDto planned = deliveryClient.planDelivery(deliveryDto).getBody();
        if (planned != null && planned.getDeliveryId() != null) {
            order.setDeliveryId(planned.getDeliveryId());
            orderRepository.save(order);
        }
    }
}