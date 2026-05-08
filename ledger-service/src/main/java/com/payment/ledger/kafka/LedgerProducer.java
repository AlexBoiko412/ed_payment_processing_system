package com.payment.ledger.kafka;

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

@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${ledger.crypto.hmac-secret}")
    private String hmacSecret;

    public void publishSettled(PaymentEvent original) {
        PaymentEvent unsigned = PaymentEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .paymentId(original.paymentId())
                .encryptedAccountId(original.encryptedAccountId())
                .encryptedDestinationAccountId(original.encryptedDestinationAccountId())
                .encryptedAmount(original.encryptedAmount())
                .currency(original.currency())
                .status(PaymentStatus.SETTLED)
                .idempotencyKey(original.idempotencyKey())
                .timestamp(Instant.now())
                .rejectionReason(null)
                .metadata(original.metadata())
                .signature(null)
                .build();

        String signature = sign(unsigned);
        PaymentEvent signed = PaymentEvent.builder()
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

        kafkaTemplate.send(KafkaTopics.PAYMENT_SETTLED, signed.paymentId(), signed);
        log.info("Published payment.settled paymentId={}", signed.paymentId());
    }

    private String sign(PaymentEvent event) {
        try {
            return HmacUtil.sign(objectMapper.writeValueAsString(event), hmacSecret);
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign ledger event", e);
        }
    }
}
