package ru.yandex.practicum.commerce.api;

import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.QuantityState;

import java.util.UUID;

public interface ShoppingStoreApi {

    @GetMapping
    ResponseEntity<Page<ProductDto>> getProducts(@RequestParam ProductCategory category,
                                                 org.springframework.data.domain.Pageable pageable);

    @GetMapping("/{productId}")
    ResponseEntity<ProductDto> getProduct(@PathVariable UUID productId);

    @PutMapping
    ResponseEntity<ProductDto> createNewProduct(@RequestBody ProductDto productDto);

    @PostMapping
    ResponseEntity<ProductDto> updateProduct(@RequestBody ProductDto productDto);

    @PostMapping("/removeProductFromStore")
    ResponseEntity<Boolean> removeProductFromStore(@RequestBody UUID productId);

    @PostMapping("/quantityState")
    ResponseEntity<Boolean> setProductQuantityState(@RequestParam UUID productId,
                                                    @RequestParam QuantityState quantityState);
}