package dev.toansh.blueprint.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "customer_id", nullable = false, updatable = false)
    private UUID customerId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "cancellation_reason")
    private String cancellationReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version")
    private Long version;

    private Order(UUID id, UUID customerId, BigDecimal amount, String currency) {
        this.id = Objects.requireNonNull(id, "Order ID must not be null");
        this.customerId = Objects.requireNonNull(customerId, "Customer ID must not be null");
        this.amount = validateAmount(amount);
        this.currency = validateCurrency(currency);
        this.status = OrderStatus.PENDING;
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Order create(UUID customerId, BigDecimal amount, String currency) {
        return new Order(UUID.randomUUID(), customerId, amount, currency);
    }

    public void confirm() {
        transitionTo(OrderStatus.CONFIRMED, null);
    }

    public void cancel(String reason) {
        Objects.requireNonNull(reason, "Cancellation reason must be provided");
        transitionTo(OrderStatus.CANCELLED, reason);
    }

    public void fail(String failureReason) {
        transitionTo(OrderStatus.FAILED, failureReason);
    }

    private void transitionTo(OrderStatus nextStatus, String reason) {
        if (!this.status.canTransitionTo(nextStatus)) {
            throw new IllegalStateException(
                "Illegal state transition from %s to %s for Order: %s".formatted(this.status, nextStatus, this.id)
            );
        }
        this.status = nextStatus;
        this.cancellationReason = reason;
        this.updatedAt = Instant.now();
    }

    private static BigDecimal validateAmount(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Order amount must be strictly positive");
        }
        return amount;
    }

    private static String validateCurrency(String currency) {
        if (currency == null || currency.trim().length() != 3) {
            throw new IllegalArgumentException("Currency must be a 3-letter ISO code");
        }
        return currency.trim().toUpperCase();
    }
}
