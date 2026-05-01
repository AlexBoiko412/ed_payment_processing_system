package com.payment.payments.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.payments.kafka.PaymentProducer;
import com.payment.shared.crypto.AesUtil;
import com.payment.shared.crypto.HmacUtil;
import com.payment.shared.dto.PaymentEvent;
import com.payment.shared.dto.PaymentRequest;
import com.payment.shared.dto.PaymentResponse;
import com.payment.shared.dto.PaymentStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Core business logic for payment initiation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final String IDEMPOTENCY_PREFIX = "idempotency:";
    private static final String DUPLICATE_MARKER   = "duplicate:";

    private final PaymentProducer      paymentProducer;
    private final StringRedisTemplate  redisTemplate;
    private final ObjectMapper         objectMapper;

    @Value("${payment.idempotency.ttl-minutes:1440}")
    private long idempotencyTtlMinutes;

    @Value("${payment.crypto.aes-key}")
    private String aesKey;

    @Value("${payment.crypto.hmac-secret}")
    private String hmacSecret;

    public PaymentResponse initiatePayment(PaymentRequest request, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            idempotencyKey = UUID.randomUUID().toString();
        }

        String redisKey = IDEMPOTENCY_PREFIX + idempotencyKey;

        String existingPaymentId = redisTemplate.opsForValue().get(redisKey);
        if (existingPaymentId != null) {
            log.info("Duplicate request detected idempotencyKey={} paymentId={}", idempotencyKey, existingPaymentId);
            markDuplicate(idempotencyKey);
            return buildResponse(existingPaymentId, idempotencyKey, PaymentStatus.INITIATED, "Duplicate - returning cached result");
        }


        String encryptedAccountId      = AesUtil.encrypt(request.accountId(), aesKey);
        String encryptedDestAccountId  = AesUtil.encrypt(request.destinationAccountId(), aesKey);
        String encryptedAmount         = AesUtil.encrypt(request.amount().toPlainString(), aesKey);


        String paymentId = UUID.randomUUID().toString();
        String eventId   = UUID.randomUUID().toString();

        PaymentEvent unsignedEvent = PaymentEvent.builder()
                .eventId(eventId)
                .paymentId(paymentId)
                .encryptedAccountId(encryptedAccountId)
                .encryptedDestinationAccountId(encryptedDestAccountId)
                .encryptedAmount(encryptedAmount)
                .currency(request.currency())
                .status(PaymentStatus.INITIATED)
                .idempotencyKey(idempotencyKey)
                .timestamp(Instant.now())
                .rejectionReason(null)
                .metadata(request.metadata())
                .signature(null)
                .build();

        String signature = signEvent(unsignedEvent);
        PaymentEvent signedEvent = PaymentEvent.builder()
                .eventId(unsignedEvent.eventId())
                .paymentId(unsignedEvent.paymentId())
                .encryptedAccountId(unsignedEvent.encryptedAccountId())
                .encryptedDestinationAccountId(unsignedEvent.encryptedDestinationAccountId())
                .encryptedAmount(unsignedEvent.encryptedAmount())
                .currency(unsignedEvent.currency())
                .status(unsignedEvent.status())
                .idempotencyKey(unsignedEvent.idempotencyKey())
                .timestamp(unsignedEvent.timestamp())
                .rejectionReason(null)
                .metadata(unsignedEvent.metadata())
                .signature(signature)
                .build();

        paymentProducer.publish(signedEvent);

        redisTemplate.opsForValue().set(
                redisKey,
                paymentId,
                Duration.ofMinutes(idempotencyTtlMinutes));

        log.info("Payment initiated paymentId={} idempotencyKey={}", paymentId, idempotencyKey);
        return buildResponse(paymentId, idempotencyKey, PaymentStatus.INITIATED, "Payment initiated successfully");
    }

    public boolean wasDuplicate(String idempotencyKey) {
        return Boolean.TRUE.equals(
                redisTemplate.hasKey(DUPLICATE_MARKER + idempotencyKey));
    }


    private void markDuplicate(String idempotencyKey) {
        redisTemplate.opsForValue().set(
                DUPLICATE_MARKER + idempotencyKey, "1", Duration.ofSeconds(5));
    }

    private String signEvent(PaymentEvent event) {
        try {
            // Serialize the event without the signature field as the canonical payload
            // TODO: use a deterministic serialiser (sorted keys) to avoid field-order issues
            String payload = objectMapper.writeValueAsString(event);
            return HmacUtil.sign(payload, hmacSecret);
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign payment event", e);
        }
    }

    private PaymentResponse buildResponse(String paymentId, String idempotencyKey,
                                          PaymentStatus status, String message) {
        return PaymentResponse.builder()
                .paymentId(paymentId)
                .idempotencyKey(idempotencyKey)
                .status(status)
                .message(message)
                .timestamp(Instant.now())
                .build();
    }
}
