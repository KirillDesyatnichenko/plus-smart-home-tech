package ru.yandex.practicum.commerce.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.dto.OrderDto;

import java.util.UUID;

public interface DeliveryApi {

    @PutMapping
    ResponseEntity<DeliveryDto> planDelivery(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/successful")
    ResponseEntity<Void> deliverySuccessful(@RequestBody UUID orderId);

    @PostMapping("/picked")
    ResponseEntity<Void> deliveryPicked(@RequestBody UUID orderId);

    @PostMapping("/failed")
    ResponseEntity<Void> deliveryFailed(@RequestBody UUID orderId);

    @PostMapping("/cost")
    ResponseEntity<Double> deliveryCost(@RequestBody OrderDto orderDto);
}