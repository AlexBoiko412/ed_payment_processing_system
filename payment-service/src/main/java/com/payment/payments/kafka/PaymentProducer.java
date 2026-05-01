package com.payment.payments.kafka;

import com.payment.shared.dto.PaymentEvent;
import com.payment.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes signed {@link PaymentEvent} messages to {@code payment.initiated}.
 *
 * Partitioning: the paymentId is used as the Kafka message key so that all
 * events for the same payment are routed to the same partition, preserving
 * ordering for downstream consumers.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public void publish(PaymentEvent event) {
        CompletableFuture<SendResult<String, PaymentEvent>> future =
                kafkaTemplate.send(KafkaTopics.PAYMENT_INITIATED, event.paymentId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish payment event paymentId={} error={}",
                        event.paymentId(), ex.getMessage(), ex);
                // TODO: implement a local outbox pattern / retry store so the event
                //       is not silently dropped on transient Kafka unavailability
            } else {
                log.info("Published payment.initiated paymentId={} partition={} offset={}",
                        event.paymentId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
