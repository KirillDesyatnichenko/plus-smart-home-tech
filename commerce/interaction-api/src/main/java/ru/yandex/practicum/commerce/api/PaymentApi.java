package ru.yandex.practicum.commerce.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.PaymentDto;

import java.util.UUID;

public interface PaymentApi {

    @PostMapping
    ResponseEntity<PaymentDto> payment(@RequestBody OrderDto orderDto);

    @PostMapping("/totalCost")
    ResponseEntity<Double> getTotalCost(@RequestBody OrderDto orderDto);

    @PostMapping("/productCost")
    ResponseEntity<Double> productCost(@RequestBody OrderDto orderDto);

    @PostMapping("/refund")
    ResponseEntity<Void> paymentSuccess(@RequestBody UUID paymentId);

    @PostMapping("/failed")
    ResponseEntity<Void> paymentFailed(@RequestBody UUID paymentId);
}