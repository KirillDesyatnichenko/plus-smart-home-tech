package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "order",
        path = "/api/v1/order",
        configuration = FeignClientConfiguration.class
)
public interface OrderClient extends OrderApi {
}