package ru.yandex.practicum.commerce.dto;

import java.util.List;

public record PageResponse<T>(List<T> content, List<SortItem> sort) {
    public record SortItem(String direction, String property) {
    }
}