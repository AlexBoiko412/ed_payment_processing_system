package com.payment.shared.kafka;

/**
 * Centralised Kafka topic name constants.
 * All services import this to avoid magic strings.
 */
public final class KafkaTopics {

    // -- Primary event flow ------------------------------------------------
    /** Published by payment-service when a payment request is accepted. */
    public static final String PAYMENT_INITIATED  = "payment.initiated";

    /** Published by fraud-service when a payment passes all fraud checks. */
    public static final String PAYMENT_VALIDATED  = "payment.validated";

    /** Published by fraud-service when a payment fails fraud checks. */
    public static final String PAYMENT_REJECTED   = "payment.rejected";

    /** Published by ledger-service when double-entry bookkeeping is complete. */
    public static final String PAYMENT_SETTLED    = "payment.settled";

    // -- Saga / compensation -----------------------------------------------
    /**
     * Published by settlement-service to trigger compensating transactions.
     * Consumers (ledger, fraud) listen here to roll back side effects.
     */
    public static final String PAYMENT_COMPENSATE = "payment.compensate";

    // -- Dead Letter Queue -------------------------------------------------
    /**
     * Messages that could not be processed after all retries land here.
     * Every consumer's DefaultErrorHandler is wired to publish here on failure.
     */
    public static final String PAYMENT_DLQ        = "payment.dlq";

    private KafkaTopics() {}
}
