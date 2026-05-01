package com.payment.shared.dto;

/**
 * Lifecycle states of a payment as it moves through the system.
 */
public enum PaymentStatus {
    /** Payment request received and idempotency key stored. */
    INITIATED,

    /** Fraud service approved the payment. */
    VALIDATED,

    /** Fraud service rejected the payment. */
    REJECTED,

    /** Ledger service recorded and balanced the transaction. */
    SETTLED,

    /**
     * Compensating transaction issued - the payment was rolled back
     * due to a downstream failure in the saga.
     */
    COMPENSATED
}
