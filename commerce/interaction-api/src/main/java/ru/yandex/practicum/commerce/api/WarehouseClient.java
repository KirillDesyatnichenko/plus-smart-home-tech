package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(
        name = "warehouse",
        path = "/api/v1/warehouse",
        configuration = FeignClientConfiguration.class,
        fallbackFactory = WarehouseClientFallbackFactory.class
)
public interface WarehouseClient extends WarehouseApi {
}