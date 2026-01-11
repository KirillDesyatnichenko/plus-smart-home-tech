package ru.yandex.practicum.commerce.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.commerce.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

public interface OrderApi {

    @GetMapping
    ResponseEntity<List<OrderDto>> getClientOrders(@RequestParam String username);

    @PutMapping
    ResponseEntity<OrderDto> createNewOrder(@RequestBody CreateNewOrderRequest request);

    @PostMapping("/return")
    ResponseEntity<OrderDto> productReturn(
            @RequestParam(name = "productReturnRequest") ProductReturnRequest productReturnRequestParam,
            @RequestBody(required = false) ProductReturnRequest productReturnRequestBody
    );

    @PostMapping("/payment")
    ResponseEntity<OrderDto> payment(@RequestBody UUID orderId);

    @PostMapping("/payment/failed")
    ResponseEntity<OrderDto> paymentFailed(@RequestBody UUID orderId);

    @PostMapping("/delivery")
    ResponseEntity<OrderDto> delivery(@RequestBody UUID orderId);

    @PostMapping("/delivery/failed")
    ResponseEntity<OrderDto> deliveryFailed(@RequestBody UUID orderId);

    @PostMapping("/completed")
    ResponseEntity<OrderDto> complete(@RequestBody UUID orderId);

    @PostMapping("/calculate/total")
    ResponseEntity<OrderDto> calculateTotalCost(@RequestBody UUID orderId);

    @PostMapping("/calculate/delivery")
    ResponseEntity<OrderDto> calculateDeliveryCost(@RequestBody UUID orderId);

    @PostMapping("/assembly")
    ResponseEntity<OrderDto> assembly(@RequestBody UUID orderId);

    @PostMapping("/assembly/failed")
    ResponseEntity<OrderDto> assemblyFailed(@RequestBody UUID orderId);

}