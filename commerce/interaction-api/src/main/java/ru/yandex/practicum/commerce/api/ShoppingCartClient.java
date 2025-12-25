package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "shopping-cart",
        path = "/api/v1/shopping-cart",
        configuration = FeignClientConfiguration.class
)
public interface ShoppingCartClient extends ShoppingCartApi {
}