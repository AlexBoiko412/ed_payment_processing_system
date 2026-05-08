package com.payment.ledger.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.ledger.kafka.LedgerProducer;
import com.payment.ledger.model.LedgerEvent;
import com.payment.ledger.model.LedgerEventType;
import com.payment.ledger.repository.LedgerEventRepository;
import com.payment.shared.crypto.AesUtil;
import com.payment.shared.crypto.HmacUtil;
import com.payment.shared.dto.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

/**
 * Event-sourced ledger implementation.
 *
 * Core invariant: balances are NEVER stored directly. The source of truth is
 * the append-only {@code ledger_events} table. The current balance for any
 * account is computed by replaying all events in chronological order.
 *
 * Double-entry: every payment produces exactly two ledger events:
 *   • DEBIT  on the source account (money out)
 *   • CREDIT on the destination account (money in)
 *
 * Compensation: produces a COMPENSATE event that reverses the original entries.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerService {

    private final LedgerEventRepository ledgerEventRepository;
    private final LedgerProducer        ledgerProducer;
    private final ObjectMapper          objectMapper;

    @Value("${ledger.crypto.aes-key}")
    private String aesKey;

    @Value("${ledger.crypto.hmac-secret}")
    private String hmacSecret;

    /**
     * Records a validated payment as a double-entry bookkeeping transaction.
     * Idempotent: if events for this idempotencyKey already exist they are skipped.
     */
    @Transactional
    public void settle(PaymentEvent event) {
        if (ledgerEventRepository
                .findByIdempotencyKeyAndEventType(event.idempotencyKey(), LedgerEventType.DEBIT)
                .isPresent()) {
            log.warn("Duplicate ledger entry skipped idempotencyKey={}", event.idempotencyKey());
            return;
        }

        LedgerEvent debit = buildEvent(event, LedgerEventType.DEBIT);
        ledgerEventRepository.save(debit);

        LedgerEvent credit = buildEvent(event, LedgerEventType.CREDIT);
        ledgerEventRepository.save(credit);

        log.info("Ledger entries written paymentId={} debitId={} creditId={}",
                event.paymentId(), debit.getId(), credit.getId());

        ledgerProducer.publishSettled(event);
    }

    /**
     * Compensates a previously settled payment by appending a COMPENSATE event.
     * Does NOT delete or modify existing rows - the reversal is a new append.
     */
    @Transactional
    public void compensate(PaymentEvent event) {
        if (ledgerEventRepository
                .findByIdempotencyKeyAndEventType(event.idempotencyKey(), LedgerEventType.COMPENSATE)
                .isPresent()) {
            log.warn("Duplicate compensation skipped idempotencyKey={}", event.idempotencyKey());
            return;
        }

        LedgerEvent compensate = buildEvent(event, LedgerEventType.COMPENSATE);
        ledgerEventRepository.save(compensate);
        log.info("Compensation entry written paymentId={}", event.paymentId());
        // TODO: publish a payment.compensated event for downstream services
    }

    /**
     * Replays all events for an account to materialise its current balance.
     * DEBIT subtracts, CREDIT adds, COMPENSATE reverses the original direction.
     *
     * NOTE: this decrypts every row - for large accounts use an incremental
     * snapshot + replay-from-snapshot pattern.
     * TODO: cache the last known snapshot balance in Redis.
     */
    public BigDecimal getBalance(String plainAccountId) {
        String encryptedAccountId = AesUtil.encrypt(plainAccountId, aesKey);
        List<LedgerEvent> events = ledgerEventRepository
                .findByEncryptedAccountIdOrderByCreatedAtAsc(encryptedAccountId);

        BigDecimal balance = BigDecimal.ZERO;
        for (LedgerEvent e : events) {
            BigDecimal amount = new BigDecimal(AesUtil.decrypt(e.getEncryptedAmount(), aesKey));
            balance = switch (e.getEventType()) {
                case DEBIT      -> balance.subtract(amount);
                case CREDIT     -> balance.add(amount);
                case COMPENSATE -> balance.add(amount);
            };
        }
        return balance;
    }

    private LedgerEvent buildEvent(PaymentEvent paymentEvent, LedgerEventType type) {
        String rowData = paymentEvent.paymentId() + type.name()
                + paymentEvent.encryptedAmount() + paymentEvent.idempotencyKey();
        String signature = HmacUtil.sign(rowData, hmacSecret);

        return LedgerEvent.builder()
                .paymentId(paymentEvent.paymentId())
                .eventType(type)
                .encryptedAccountId(paymentEvent.encryptedAccountId())
                .encryptedDestinationAccountId(paymentEvent.encryptedDestinationAccountId())
                .encryptedAmount(paymentEvent.encryptedAmount())
                .encryptedBalanceAfter(null)   // TODO: compute and store running balance
                .currency(paymentEvent.currency())
                .idempotencyKey(paymentEvent.idempotencyKey())
                .signature(signature)
                .build();
    }
}
