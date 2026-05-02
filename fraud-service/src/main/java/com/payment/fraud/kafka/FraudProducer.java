package com.payment.fraud.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.shared.crypto.HmacUtil;
import com.payment.shared.dto.PaymentEvent;
import com.payment.shared.dto.PaymentStatus;
import com.payment.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Publishes fraud decision events to {@code payment.validated} or {@code payment.rejected}.
 * Each outbound event is re-signed with a fresh HMAC so downstream consumers can verify it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${fraud.crypto.hmac-secret}")
    private String hmacSecret;

    public void publishValidated(PaymentEvent original) {
        PaymentEvent event = buildEvent(original, PaymentStatus.VALIDATED, null);
        kafkaTemplate.send(KafkaTopics.PAYMENT_VALIDATED, event.paymentId(), event);
        log.info("Published payment.validated paymentId={}", event.paymentId());
    }

    public void publishRejected(PaymentEvent original, String reason) {
        PaymentEvent event = buildEvent(original, PaymentStatus.REJECTED, reason);
        kafkaTemplate.send(KafkaTopics.PAYMENT_REJECTED, event.paymentId(), event);
        log.info("Published payment.rejected paymentId={} reason={}", event.paymentId(), reason);
    }

    private PaymentEvent buildEvent(PaymentEvent original, PaymentStatus status, String reason) {
        PaymentEvent unsigned = PaymentEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .paymentId(original.paymentId())
                .encryptedAccountId(original.encryptedAccountId())
                .encryptedDestinationAccountId(original.encryptedDestinationAccountId())
                .encryptedAmount(original.encryptedAmount())
                .currency(original.currency())
                .status(status)
                .idempotencyKey(original.idempotencyKey())
                .timestamp(Instant.now())
                .rejectionReason(reason)
                .metadata(original.metadata())
                .signature(null)
                .build();

        String signature = sign(unsigned);
        return PaymentEvent.builder()
                .eventId(unsigned.eventId())
                .paymentId(unsigned.paymentId())
                .encryptedAccountId(unsigned.encryptedAccountId())
                .encryptedDestinationAccountId(unsigned.encryptedDestinationAccountId())
                .encryptedAmount(unsigned.encryptedAmount())
                .currency(unsigned.currency())
                .status(unsigned.status())
                .idempotencyKey(unsigned.idempotencyKey())
                .timestamp(unsigned.timestamp())
                .rejectionReason(unsigned.rejectionReason())
                .metadata(unsigned.metadata())
                .signature(signature)
                .build();
    }

    private String sign(PaymentEvent event) {
        try {
            return HmacUtil.sign(objectMapper.writeValueAsString(event), hmacSecret);
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign fraud event", e);
        }
    }
}
