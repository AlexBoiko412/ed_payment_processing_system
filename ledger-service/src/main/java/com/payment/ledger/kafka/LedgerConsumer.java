package com.payment.ledger.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.ledger.service.LedgerService;
import com.payment.shared.crypto.HmacUtil;
import com.payment.shared.dto.PaymentEvent;
import com.payment.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code payment.validated} events and appends double-entry ledger records.
 * Also listens to {@code payment.compensate} to record reversals.
 *
 * DLQ: unprocessable messages after retries go to {@code payment.dlq} via the
 * error handler in {@link LedgerKafkaConfig}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerConsumer {

    private final LedgerService ledgerService;
    private final ObjectMapper  objectMapper;

    @Value("${ledger.crypto.hmac-secret}")
    private String hmacSecret;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_VALIDATED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "ledgerKafkaListenerContainerFactory"
    )
    public void onPaymentValidated(
            @Payload PaymentEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("Received payment.validated paymentId={} partition={} offset={}",
                event.paymentId(), partition, offset);

        if (!verifySignature(event)) {
            log.error("SECURITY: HMAC verification FAILED paymentId={}. Discarding.", event.paymentId());
            ack.acknowledge();
            return;
        }

        ledgerService.settle(event);
        ack.acknowledge();
    }

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_COMPENSATE,
            groupId = "${spring.kafka.consumer.group-id}-compensate",
            containerFactory = "ledgerKafkaListenerContainerFactory"
    )
    public void onCompensate(
            @Payload PaymentEvent event,
            Acknowledgment ack) {

        log.info("Received payment.compensate paymentId={}", event.paymentId());

        if (!verifySignature(event)) {
            log.error("SECURITY: HMAC verification FAILED on compensate paymentId={}. Discarding.", event.paymentId());
            ack.acknowledge();
            return;
        }

        ledgerService.compensate(event);
        ack.acknowledge();
    }

    private boolean verifySignature(PaymentEvent event) {
        try {
            PaymentEvent withoutSig = PaymentEvent.builder()
                    .eventId(event.eventId())
                    .paymentId(event.paymentId())
                    .encryptedAccountId(event.encryptedAccountId())
                    .encryptedDestinationAccountId(event.encryptedDestinationAccountId())
                    .encryptedAmount(event.encryptedAmount())
                    .currency(event.currency())
                    .status(event.status())
                    .idempotencyKey(event.idempotencyKey())
                    .timestamp(event.timestamp())
                    .rejectionReason(event.rejectionReason())
                    .metadata(event.metadata())
                    .signature(null)
                    .build();
            String payload = objectMapper.writeValueAsString(withoutSig);
            return HmacUtil.verify(payload, event.signature(), hmacSecret);
        } catch (Exception e) {
            log.error("Signature verification error paymentId={}", event.paymentId(), e);
            return false;
        }
    }
}
