package ru.yandex.practicum.commerce.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import ru.yandex.practicum.commerce.constants.Messages;
import ru.yandex.practicum.commerce.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.shoppingcart.mapper.ShoppingCartMapper;
import ru.yandex.practicum.commerce.shoppingcart.model.ShoppingCart;
import ru.yandex.practicum.commerce.shoppingcart.model.ShoppingCartState;
import ru.yandex.practicum.commerce.shoppingcart.repository.ShoppingCartRepository;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ShoppingCartTransactionalService {
    private final ShoppingCartRepository shoppingCartRepository;
    private final ShoppingCartMapper shoppingCartMapper;

    @Transactional(readOnly = true)
    public ShoppingCartDto getShoppingCart(String username) {
        ShoppingCart cart = getOrCreateActiveCart(username);
        return shoppingCartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductToShoppingCart(String username, Map<UUID, Long> productsToAdd) {
        ShoppingCart cart = getOrCreateActiveCart(username);
        mergeProducts(cart, productsToAdd);
        shoppingCartRepository.save(cart);
        return shoppingCartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto removeFromShoppingCart(String username, Iterable<UUID> productIds) {
        ShoppingCart cart = getOrCreateActiveCart(username);
        boolean removed = false;
        for (UUID productId : productIds) {
            if (cart.getProducts().remove(productId) != null) {
                removed = true;
            }
        }
        if (!removed) {
            throw new NoProductsInShoppingCartException();
        }
        shoppingCartRepository.save(cart);
        return shoppingCartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        ShoppingCart cart = getOrCreateActiveCart(username);
        cart.getProducts().computeIfPresent(request.getProductId(), (k, v) -> request.getNewQuantity());
        if (!cart.getProducts().containsKey(request.getProductId())) {
            throw new NoProductsInShoppingCartException();
        }
        shoppingCartRepository.save(cart);
        return shoppingCartMapper.toDto(cart);
    }

    @Transactional
    public void deactivateCurrentShoppingCart(String username) {
        validateUsername(username);
        ShoppingCart cart = shoppingCartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> ShoppingCart.builder()
                        .username(username)
                        .state(ShoppingCartState.DEACTIVATED)
                        .build());
        cart.setState(ShoppingCartState.DEACTIVATED);
        shoppingCartRepository.save(cart);
    }

    @Transactional(readOnly = true)
    public ShoppingCartDto getShoppingCartForValidation(String username) {
        ShoppingCart cart = getOrCreateActiveCart(username);
        return shoppingCartMapper.toDto(cart);
    }

    private ShoppingCart getOrCreateActiveCart(String username) {
        validateUsername(username);
        return shoppingCartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> shoppingCartRepository.save(ShoppingCart.builder()
                        .username(username)
                        .state(ShoppingCartState.ACTIVE)
                        .build()));
    }

    private void validateUsername(String username) {
        if (!StringUtils.hasText(username)) {
            throw new NotAuthorizedUserException();
        }
    }

    private void mergeProducts(ShoppingCart cart, Map<UUID, Long> productsToAdd) {
        productsToAdd.forEach((productId, quantity) -> {
            if (quantity == null || quantity <= 0) {
                throw new IllegalArgumentException(Messages.QUANTITY_POSITIVE);
            }
            cart.getProducts().merge(productId, quantity, Long::sum);
        });
    }
}