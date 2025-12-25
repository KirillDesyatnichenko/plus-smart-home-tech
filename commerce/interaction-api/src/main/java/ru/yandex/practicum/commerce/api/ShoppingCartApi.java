package ru.yandex.practicum.commerce.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.commerce.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ShoppingCartApi {

    @GetMapping
    ResponseEntity<ShoppingCartDto> getShoppingCart(@RequestParam String username);

    @PutMapping
    ResponseEntity<ShoppingCartDto> addProductToShoppingCart(@RequestParam String username,
                                                             @RequestBody Map<UUID, Long> products);

    @DeleteMapping
    ResponseEntity<Void> deactivateCurrentShoppingCart(@RequestParam String username);

    @PostMapping("/remove")
    ResponseEntity<ShoppingCartDto> removeFromShoppingCart(@RequestParam String username,
                                                           @RequestBody List<UUID> productIds);

    @PostMapping("/change-quantity")
    ResponseEntity<ShoppingCartDto> changeProductQuantity(@RequestParam String username,
                                                          @RequestBody ChangeProductQuantityRequest request);
}