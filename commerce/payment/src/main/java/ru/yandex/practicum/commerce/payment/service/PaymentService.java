package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.PaymentDto;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.commerce.payment.model.PaymentStatus;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final double DELIVERY_FLAT_COST = 50.0;
    private static final double VAT_RATE = 0.1;

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    @Transactional
    public PaymentDto payment(OrderDto orderDto) {
        validateOrder(orderDto);
        log.info("Формирование оплаты для заказа {}", orderDto.getOrderId());
        double productCost = productCost(orderDto);
        double totalCost = totalCost(orderDto, productCost);

        Payment payment = Payment.builder()
                .orderId(orderDto.getOrderId())
                .productCost(productCost)
                .deliveryCost(resolveDeliveryCost(orderDto))
                .feeTotal(productCost * VAT_RATE)
                .totalPayment(totalCost)
                .status(PaymentStatus.PENDING)
                .build();
        Payment saved = paymentRepository.save(payment);
        PaymentDto result = paymentMapper.toDto(saved);
        log.info("Оплата сформирована: {}", result);
        return result;
    }

    @Transactional(readOnly = true)
    public double productCost(OrderDto orderDto) {
        validateOrder(orderDto);
        log.info("Расчёт стоимости товаров для заказа {}", orderDto.getOrderId());
        Map<UUID, Long> products = orderDto.getProducts();
        if (CollectionUtils.isEmpty(products)) {
            throw new NotEnoughInfoInOrderToCalculateException();
        }
        return products.entrySet().stream()
                .mapToDouble(entry -> {
                    ProductDto product = shoppingStoreClient.getProductInternal(entry.getKey()).getBody();
                    double price = product != null && product.getPrice() != null ? product.getPrice() : 0d;
                    return price * entry.getValue();
                }).sum();
    }

    @Transactional(readOnly = true)
    public double totalCost(OrderDto orderDto, double productCost) {
        double delivery = resolveDeliveryCost(orderDto);
        double vat = productCost * VAT_RATE;
        double total = productCost + vat + delivery;
        log.info("Полная стоимость заказа {}: товары={}, НДС={}, доставка={}, итого={}",
                orderDto.getOrderId(), productCost, vat, delivery, total);
        return total;
    }

    @Transactional
    public void paymentSuccess(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(NoOrderFoundException::new);
        payment.setStatus(PaymentStatus.SUCCESS);
        paymentRepository.save(payment);
        if (payment.getOrderId() != null) {
            orderClient.payment(payment.getOrderId());
        }
        log.info("Оплата {} успешна, статус обновлён", paymentId);
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(NoOrderFoundException::new);
        payment.setStatus(PaymentStatus.FAILED);
        paymentRepository.save(payment);
        if (payment.getOrderId() != null) {
            orderClient.paymentFailed(payment.getOrderId());
        }
        log.info("Оплата {} завершилась ошибкой, статус обновлён", paymentId);
    }

    private void validateOrder(OrderDto orderDto) {
        if (orderDto == null || orderDto.getProducts() == null) {
            throw new NotEnoughInfoInOrderToCalculateException();
        }
    }

    private double resolveDeliveryCost(OrderDto orderDto) {
        return orderDto.getDeliveryPrice() != null ? orderDto.getDeliveryPrice() : DELIVERY_FLAT_COST;
    }
}