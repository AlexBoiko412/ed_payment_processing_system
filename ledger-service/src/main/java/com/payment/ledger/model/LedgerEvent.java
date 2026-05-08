package com.payment.ledger.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity for the {@code ledger_events} table.
 *
 * All sensitive columns are stored encrypted - decrypt with AesUtil before using.
 * The entity is intentionally immutable after construction; use the
 * {@link Builder} and never call setters.
 */
@Entity
@Table(name = "ledger_events")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class LedgerEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "payment_id", nullable = false, length = 36)
    private String paymentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 20)
    private LedgerEventType eventType;

    @Column(name = "encrypted_account_id", nullable = false, columnDefinition = "TEXT")
    private String encryptedAccountId;

    @Column(name = "encrypted_destination_account_id", nullable = false, columnDefinition = "TEXT")
    private String encryptedDestinationAccountId;

    @Column(name = "encrypted_amount", nullable = false, columnDefinition = "TEXT")
    private String encryptedAmount;

    /**
     * AES-256-GCM encrypted running balance after this event was applied.
     * Null until the materialised balance view is populated.
     * TODO: populate during LedgerService.applyEvent()
     */
    @Column(name = "encrypted_balance_after", columnDefinition = "TEXT")
    private String encryptedBalanceAfter;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "idempotency_key", nullable = false, length = 36)
    private String idempotencyKey;

    @Column(name = "signature", nullable = false, columnDefinition = "TEXT")
    private String signature;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
