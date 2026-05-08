package com.payment.ledger.repository;

import com.payment.ledger.model.LedgerEvent;
import com.payment.ledger.model.LedgerEventType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface LedgerEventRepository extends JpaRepository<LedgerEvent, UUID> {

    @Query("SELECT e FROM LedgerEvent e WHERE e.encryptedAccountId = :encryptedAccountId ORDER BY e.createdAt ASC")
    List<LedgerEvent> findByEncryptedAccountIdOrderByCreatedAtAsc(
            @Param("encryptedAccountId") String encryptedAccountId);

    Optional<LedgerEvent> findByIdempotencyKeyAndEventType(String idempotencyKey, LedgerEventType eventType);

    List<LedgerEvent> findByPaymentIdOrderByCreatedAtAsc(String paymentId);

    // TODO: add a query that decrypts and aggregates balances - this requires
    //       either database-level decryption (pgcrypto) or application-level replay.
}
