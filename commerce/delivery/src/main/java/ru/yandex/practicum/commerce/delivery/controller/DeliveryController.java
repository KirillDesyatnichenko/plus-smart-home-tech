package ru.yandex.practicum.commerce.delivery.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.DeliveryApi;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController implements DeliveryApi {

    private final DeliveryService deliveryService;

    @Override
    public ResponseEntity<DeliveryDto> planDelivery(@Valid @RequestBody DeliveryDto deliveryDto) {
        return ResponseEntity.ok(deliveryService.planDelivery(deliveryDto));
    }

    @Override
    public ResponseEntity<Void> deliverySuccessful(@RequestBody UUID orderId) {
        deliveryService.deliverySuccessful(orderId);
        return ResponseEntity.noContent().build();
    }

    @Override
    public ResponseEntity<Void> deliveryPicked(@RequestBody UUID orderId) {
        deliveryService.deliveryPicked(orderId);
        return ResponseEntity.noContent().build();
    }

    @Override
    public ResponseEntity<Void> deliveryFailed(@RequestBody UUID orderId) {
        deliveryService.deliveryFailed(orderId);
        return ResponseEntity.noContent().build();
    }

    @Override
    public ResponseEntity<Double> deliveryCost(@Valid @RequestBody OrderDto orderDto) {
        return ResponseEntity.ok(deliveryService.calculateCost(orderDto));
    }
}