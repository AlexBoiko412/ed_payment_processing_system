package com.payment.shared.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

import java.time.Instant;

/**
 * REST response returned to the caller by payment-service.
 * Does NOT include any sensitive field values - only IDs and status.
 */
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentResponse(

        String paymentId,

        String idempotencyKey,

        PaymentStatus status,

        /** Human-readable message (e.g. "Payment initiated successfully"). */
        String message,

        Instant timestamp
) {}
