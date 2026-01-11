package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "payment",
        path = "/api/v1/payment",
        configuration = FeignClientConfiguration.class
)
public interface PaymentClient extends PaymentApi {
}