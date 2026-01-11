package ru.yandex.practicum.commerce.order.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.commerce.api.OrderApi;
import ru.yandex.practicum.commerce.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.ProductReturnRequest;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController implements OrderApi {

    private final OrderService orderService;

    @Override
    public ResponseEntity<List<OrderDto>> getClientOrders(@RequestParam String username) {
        return ResponseEntity.ok(orderService.getClientOrders(username));
    }

    @Override
    public ResponseEntity<OrderDto> createNewOrder(@Valid @RequestBody CreateNewOrderRequest request) {
        return ResponseEntity.ok(orderService.createNewOrder(request));
    }

    @Override
    public ResponseEntity<OrderDto> productReturn(
            @RequestParam(name = "productReturnRequest") ProductReturnRequest productReturnRequestParam,
            @Valid @RequestBody(required = false) ProductReturnRequest productReturnRequestBody
    ) {
        ProductReturnRequest request = productReturnRequestBody != null ? productReturnRequestBody : productReturnRequestParam;
        return ResponseEntity.ok(orderService.productReturn(request));
    }

    @Override
    public ResponseEntity<OrderDto> payment(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.payment(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> paymentFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.paymentFailed(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> delivery(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.delivery(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> deliveryFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.deliveryFailed(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> complete(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.complete(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> calculateTotalCost(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.calculateTotalCost(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> calculateDeliveryCost(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.calculateDeliveryCost(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> assembly(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.assembly(orderId));
    }

    @Override
    public ResponseEntity<OrderDto> assemblyFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.assemblyFailed(orderId));
    }
}