package dev.toansh.blueprint.infrastructure.web.dto;

import dev.toansh.blueprint.domain.model.Order;
import dev.toansh.blueprint.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        UUID customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status,
        String cancellationReason,
        Instant createdAt,
        Instant updatedAt
) {
    public static OrderResponse fromDomain(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getAmount(),
                order.getCurrency(),
                order.getStatus(),
                order.getCancellationReason(),
                order.getCreatedAt(),
                order.getUpdatedAt()
        );
    }
}
