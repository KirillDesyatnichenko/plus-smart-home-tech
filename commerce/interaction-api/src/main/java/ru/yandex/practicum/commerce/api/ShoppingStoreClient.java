package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "shopping-store",
        path = "/api/v1/shopping-store",
        configuration = FeignClientConfiguration.class
)
public interface ShoppingStoreClient extends ShoppingStoreApi {
}