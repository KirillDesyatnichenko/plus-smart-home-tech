package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "delivery",
        path = "/api/v1/delivery",
        configuration = FeignClientConfiguration.class
)
public interface DeliveryClient extends DeliveryApi {
}