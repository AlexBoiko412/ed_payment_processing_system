package com.payment.fraud.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.fraud.service.FraudRuleEngine;
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
 * Consumes {@code payment.initiated} events, runs the fraud rule engine,
 * and publishes {@code payment.validated} or {@code payment.rejected}.
 *
 * Signature verification is performed before any processing to prevent
 * tampered messages from affecting the fraud decision.
 *
 * DLQ: messages that fail after all retries are routed to {@code payment.dlq}
 * by the {@code DefaultErrorHandler} configured in {@link FraudKafkaConfig}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudConsumer {

    private final FraudRuleEngine  fraudRuleEngine;
    private final FraudProducer    fraudProducer;
    private final ObjectMapper     objectMapper;

    @Value("${fraud.crypto.hmac-secret}")
    private String hmacSecret;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_INITIATED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "fraudKafkaListenerContainerFactory"
    )
    public void onPaymentInitiated(
            @Payload PaymentEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("Received payment.initiated paymentId={} partition={} offset={}",
                event.paymentId(), partition, offset);

        if (!verifySignature(event)) {
            log.error("SECURITY: HMAC signature verification FAILED for paymentId={}. Discarding message.",
                    event.paymentId());
            ack.acknowledge();
            return;
        }

        FraudRuleEngine.FraudResult result = fraudRuleEngine.evaluate(event);

        if (result.passed()) {
            fraudProducer.publishValidated(event);
        } else {
            fraudProducer.publishRejected(event, result.reason());
        }

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
            log.error("Error during signature verification paymentId={}", event.paymentId(), e);
            return false;
        }
    }
}
