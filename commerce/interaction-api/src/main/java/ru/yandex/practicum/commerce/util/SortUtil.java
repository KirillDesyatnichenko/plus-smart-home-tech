package ru.yandex.practicum.commerce.util;

import org.springframework.data.domain.Sort;

import java.util.List;

public final class SortUtil {
    private SortUtil() {
    }

    public static Sort from(List<String> sort) {
        if (sort == null || sort.isEmpty()) {
            return Sort.unsorted();
        }
        List<Sort.Order> orders = sort.stream()
                .map(s -> {
                    String[] parts = s.split(",");
                    String property = parts[0];
                    Sort.Direction direction = parts.length > 1 && "desc".equalsIgnoreCase(parts[1])
                            ? Sort.Direction.DESC
                            : Sort.Direction.ASC;
                    return new Sort.Order(direction, property);
                })
                .toList();
        return Sort.by(orders);
    }
}