package ru.yandex.practicum.commerce.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {
    private UUID productId;

    @NotBlank(message = "Название товара обязательно")
    private String productName;

    @NotBlank(message = "Описание товара обязательно")
    private String description;

    private String imageSrc;

    @NotNull(message = "Статус остатков обязателен")
    private QuantityState quantityState;

    @NotNull(message = "Статус товара обязателен")
    private ProductState productState;

    private ProductCategory productCategory;

    @NotNull(message = "Цена обязательна")
    @Min(value = 1, message = "Цена должна быть не меньше 1")
    private Double price;
}