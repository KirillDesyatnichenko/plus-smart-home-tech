package ru.yandex.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.commerce.dto.DeliveryState;
import ru.yandex.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.exception.NoDeliveryFoundException;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private static final double BASE_COST = 5.0;

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final OrderClient orderClient;
    private final WarehouseClient warehouseClient;

    @Transactional
    public DeliveryDto planDelivery(DeliveryDto deliveryDto) {
        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        if (delivery.getDeliveryState() == null) {
            delivery.setDeliveryState(DeliveryState.CREATED);
        }
        Delivery saved = deliveryRepository.save(delivery);
        DeliveryDto result = deliveryMapper.toDto(saved);
        log.info("Создана доставка {} для заказа {}", result.getDeliveryId(), result.getOrderId());
        return result;
    }

    @Transactional
    public void deliverySuccessful(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(NoDeliveryFoundException::new);
        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);
        orderClient.delivery(orderId);
        log.info("Доставка заказа {} отмечена как успешная", orderId);
    }

    @Transactional
    public void deliveryFailed(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(NoDeliveryFoundException::new);
        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);
        orderClient.deliveryFailed(orderId);
        log.info("Доставка заказа {} отмечена как неудачная", orderId);
    }

    @Transactional
    public void deliveryPicked(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(NoDeliveryFoundException::new);
        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);
        warehouseClient.shippedToDelivery(new ShippedToDeliveryRequest(orderId, delivery.getDeliveryId()));
        orderClient.assembly(orderId);
        log.info("Заказ {} передан в доставку, доставка {}", orderId, delivery.getDeliveryId());
    }

    @Transactional(readOnly = true)
    public double calculateCost(OrderDto orderDto) {
        if (orderDto == null || orderDto.getOrderId() == null) {
            throw new NoDeliveryFoundException();
        }
        Delivery delivery = deliveryRepository.findByOrderId(orderDto.getOrderId())
                .orElseThrow(NoDeliveryFoundException::new);
        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress().getBody();
        String warehouseStreet = warehouseAddress != null ? warehouseAddress.getStreet() : "";
        double cost = BASE_COST;
        if (warehouseStreet != null && warehouseStreet.contains("ADDRESS_2")) {
            cost = cost * 2 + BASE_COST;
        } else {
            cost = cost * 1 + BASE_COST;
        }
        boolean fragile = orderDto.getFragile() != null ? orderDto.getFragile()
                : Boolean.TRUE.equals(delivery.getFragile());
        if (fragile) {
            cost = cost + cost * 0.2;
        }
        double weight = orderDto.getDeliveryWeight() != null ? orderDto.getDeliveryWeight()
                : safe(delivery.getDeliveryWeight());
        double volume = orderDto.getDeliveryVolume() != null ? orderDto.getDeliveryVolume()
                : safe(delivery.getDeliveryVolume());
        cost = cost + weight * 0.3;
        cost = cost + volume * 0.2;
        String toStreet = delivery.getToAddress() != null ? delivery.getToAddress().getStreet() : null;
        if (warehouseStreet != null && toStreet != null && !warehouseStreet.equalsIgnoreCase(toStreet)) {
            cost = cost + cost * 0.2;
        }
        log.info("Рассчитана стоимость доставки для заказа {}: {}", orderDto.getOrderId(), cost);
        return cost;
    }

    private double safe(Double value) {
        return value == null ? 0d : value;
    }
}