package ru.yandex.practicum.commerce.delivery.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.Mappings;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.delivery.model.Delivery;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface DeliveryMapper {

    @Mappings({
            @Mapping(target = "fromAddress", expression = "java(toDto(delivery.getFromAddress()))"),
            @Mapping(target = "toAddress", expression = "java(toDto(delivery.getToAddress()))")
    })
    DeliveryDto toDto(Delivery delivery);

    @Mappings({
            @Mapping(target = "fromAddress", expression = "java(toEntity(dto.getFromAddress()))"),
            @Mapping(target = "toAddress", expression = "java(toEntity(dto.getToAddress()))")
    })
    Delivery toEntity(DeliveryDto dto);

    default AddressDto toDto(Delivery.Address address) {
        if (address == null) {
            return null;
        }
        return AddressDto.builder()
                .country(address.getCountry())
                .city(address.getCity())
                .street(address.getStreet())
                .house(address.getHouse())
                .flat(address.getFlat())
                .build();
    }

    default Delivery.Address toEntity(AddressDto dto) {
        if (dto == null) {
            return null;
        }
        return Delivery.Address.builder()
                .country(dto.getCountry())
                .city(dto.getCity())
                .street(dto.getStreet())
                .house(dto.getHouse())
                .flat(dto.getFlat())
                .build();
    }
}