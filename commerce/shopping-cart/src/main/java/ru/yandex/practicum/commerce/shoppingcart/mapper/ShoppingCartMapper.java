package ru.yandex.practicum.commerce.shoppingcart.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.model.ShoppingCart;

@Mapper(componentModel = "spring")
public interface ShoppingCartMapper {
    ShoppingCartDto toDto(ShoppingCart cart);
}