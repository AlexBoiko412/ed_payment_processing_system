package com.payment.shared.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import lombok.Builder;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka message envelope shared across all services.
 *
 * Security notes:
 *  - {@code encryptedAccountId} and {@code encryptedDestinationAccountId} are
 *    AES-256-GCM encrypted (Base64-encoded ciphertext + prepended IV).
 *  - {@code encryptedAmount} is also AES-256-GCM encrypted - store/log the
 *    plaintext amount nowhere outside of the decrypting service.
 *  - {@code signature} is HMAC-SHA256 over the canonical JSON of all other
 *    fields (excluding signature itself). Consumers MUST verify before processing.
 */
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentEvent(

        /** Unique event ID (UUID v4) - for idempotent consumers. */
        String eventId,

        /** Business payment ID (UUID v4) - stable across all events for one payment. */
        String paymentId,

        /** AES-256-GCM encrypted source account identifier. */
        String encryptedAccountId,

        /** AES-256-GCM encrypted destination account identifier. */
        String encryptedDestinationAccountId,

        /**
         * AES-256-GCM encrypted amount (plaintext is BigDecimal.toPlainString()).
         * Fraud service decrypts this to apply amount-based rules.
         */
        String encryptedAmount,

        /** ISO 4217 currency code - not considered sensitive. */
        String currency,

        /** Current lifecycle status of the payment. */
        PaymentStatus status,

        /** Idempotency key used for deduplication (stored in Redis). */
        String idempotencyKey,

        @JsonSerialize(using = InstantSerializer.class)
        Instant timestamp,

        /**
         * HMAC-SHA256 signature of the canonical payload (all fields except
         * this one, serialised as sorted-key JSON).
         * Consumers call {@code HmacUtil.verify()} before processing.
         */
        String signature,

        /**
         * Reason populated by fraud-service on REJECTED events, or by
         * settlement-service on COMPENSATED events.
         */
        String rejectionReason,

        /** Pass-through metadata from the original PaymentRequest. */
        Map<String, String> metadata
) {}
