package ru.yandex.practicum.commerce.payment.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.PaymentApi;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.PaymentDto;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
public class PaymentController implements PaymentApi {

    private final PaymentService paymentService;

    @Override
    public ResponseEntity<PaymentDto> payment(@Valid @RequestBody OrderDto orderDto) {
        return ResponseEntity.ok(paymentService.payment(orderDto));
    }

    @Override
    public ResponseEntity<Double> getTotalCost(@Valid @RequestBody OrderDto orderDto) {
        double product = paymentService.productCost(orderDto);
        return ResponseEntity.ok(paymentService.totalCost(orderDto, product));
    }

    @Override
    public ResponseEntity<Double> productCost(@Valid @RequestBody OrderDto orderDto) {
        return ResponseEntity.ok(paymentService.productCost(orderDto));
    }

    @Override
    public ResponseEntity<Void> paymentSuccess(@RequestBody UUID paymentId) {
        paymentService.paymentSuccess(paymentId);
        return ResponseEntity.noContent().build();
    }

    @Override
    public ResponseEntity<Void> paymentFailed(@RequestBody UUID paymentId) {
        paymentService.paymentFailed(paymentId);
        return ResponseEntity.noContent().build();
    }
}