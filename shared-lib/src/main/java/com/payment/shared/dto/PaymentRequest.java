package com.payment.shared.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.*;
import lombok.Builder;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Inbound REST request body received by payment-service.
 * Sensitive fields (accountId, destinationAccountId, amount) are encrypted
 * before being stored or forwarded.
 */
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentRequest(

        @NotBlank(message = "accountId is required")
        String accountId,

        @NotBlank(message = "destinationAccountId is required")
        String destinationAccountId,

        @NotNull(message = "amount is required")
        @DecimalMin(value = "0.01", message = "amount must be greater than 0")
        @Digits(integer = 15, fraction = 2, message = "amount must have at most 2 decimal places")
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO 4217 code")
        String currency,

        /**
         * Client-supplied idempotency key (UUID v4).
         * If null, payment-service generates one and stores it in Redis with TTL.
         */
        String idempotencyKey,

        /**
         * Arbitrary key/value metadata (e.g. merchant category, reference number).
         * TODO: define a strict schema per payment type.
         */
        Map<String, String> metadata
) {}
