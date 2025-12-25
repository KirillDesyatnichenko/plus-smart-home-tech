package ru.yandex.practicum.commerce.shoppingstore.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.shoppingstore.model.Product;

@Mapper(componentModel = "spring")
public interface ProductMapper {
    ProductDto toDto(Product product);
    Product toEntity(ProductDto dto);
}