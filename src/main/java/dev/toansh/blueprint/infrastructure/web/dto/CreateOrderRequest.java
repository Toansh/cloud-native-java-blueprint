package dev.toansh.blueprint.infrastructure.web.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderRequest(
        @NotNull(message = "customerId is mandatory")
        UUID customerId,

        @NotNull(message = "amount is mandatory")
        @DecimalMin(value = "0.01", inclusive = true, message = "amount must be greater than zero")
        BigDecimal amount,

        @NotBlank(message = "currency is mandatory")
        @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be a valid 3-letter ISO code")
        String currency
) {
}
