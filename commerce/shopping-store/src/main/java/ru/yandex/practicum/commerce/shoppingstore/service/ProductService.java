package ru.yandex.practicum.commerce.shoppingstore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.constants.Messages;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.ProductState;
import ru.yandex.practicum.commerce.dto.QuantityState;
import ru.yandex.practicum.commerce.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.shoppingstore.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shoppingstore.model.Product;
import ru.yandex.practicum.commerce.shoppingstore.repository.ProductRepository;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        log.info("Получение товаров по категории {} (pageable={})", category, pageable);
        Page<Product> products = productRepository.findByProductCategory(category, pageable);
        Page<ProductDto> result = products.map(productMapper::toDto);
        log.info("Найдено {} товаров для категории {}", result.getTotalElements(), category);
        return result;
    }

    public ProductDto getProductById(UUID productId) {
        log.info("Получение товара по id={}", productId);
        Product product = productRepository.findByProductId(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));
        ProductDto result = productMapper.toDto(product);
        log.info("Товар найден: {}", result);
        return result;
    }

    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Создание нового товара: {}", productDto);
        Product product = productMapper.toEntity(productDto);
        Product saved = productRepository.save(product);
        ProductDto result = productMapper.toDto(saved);
        log.info("Товар создан: {}", result);
        return result;
    }

    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Обновление товара: {}", productDto);
        UUID productId = productDto.getProductId();
        if (productId == null) {
            throw new IllegalArgumentException(Messages.PRODUCT_ID_REQUIRED_FOR_UPDATE);
        }

        Product existingProduct = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        existingProduct.setProductName(productDto.getProductName());
        existingProduct.setDescription(productDto.getDescription());
        existingProduct.setImageSrc(productDto.getImageSrc());
        existingProduct.setQuantityState(productDto.getQuantityState());
        existingProduct.setProductCategory(productDto.getProductCategory());
        existingProduct.setPrice(productDto.getPrice());

        Product saved = productRepository.save(existingProduct);
        ProductDto result = productMapper.toDto(saved);
        log.info("Товар обновлен: {}", result);
        return result;
    }

    @Transactional
    public boolean removeProductFromStore(UUID productId) {
        log.info("Снятие товара с продажи: {}", productId);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Товар снят с продажи: {}", productId);
        return true;
    }

    @Transactional
    public boolean setProductQuantityState(UUID productId, QuantityState quantityState) {
        log.info("Изменение статуса остатков: productId={}, quantityState={}", productId, quantityState);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        product.setQuantityState(quantityState);
        productRepository.save(product);
        log.info("Статус остатков обновлен: productId={}, quantityState={}", productId, quantityState);
        return true;
    }
}