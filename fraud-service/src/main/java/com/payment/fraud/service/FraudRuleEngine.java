package com.payment.fraud.service;

import com.payment.shared.crypto.AesUtil;
import com.payment.shared.dto.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Rule-based fraud detection engine.
 *
 * Rules evaluated in order (first match wins):
 *  1. Amount threshold - flag if decrypted amount > {@code fraud.rules.max-amount}
 *  2. Velocity check - flag if same account has > N transactions in a sliding window
 *
 * TODO: replace with a proper rules engine (Drools / Easy Rules) or ML scoring model.
 * TODO: add device fingerprint, geolocation, and merchant-category checks.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FraudRuleEngine {

    private static final String VELOCITY_PREFIX = "velocity:";

    private final StringRedisTemplate redisTemplate;

    @Value("${fraud.rules.max-amount:10000.00}")
    private BigDecimal maxAmount;

    @Value("${fraud.rules.velocity-window-seconds:60}")
    private long velocityWindowSeconds;

    @Value("${fraud.rules.velocity-max-transactions:5}")
    private long velocityMaxTransactions;

    @Value("${fraud.crypto.aes-key}")
    private String aesKey;

    /**
     * Evaluates all rules for the given payment event.
     *
     * @return {@link FraudResult#PASS} if clean, {@link FraudResult#FAIL} with a reason if flagged
     */
    public FraudResult evaluate(PaymentEvent event) {
        BigDecimal amount = decryptAmount(event);
        if (amount.compareTo(maxAmount) > 0) {
            log.warn("FRAUD RULE 1 TRIGGERED: amount {} exceeds threshold {} paymentId={}",
                    amount, maxAmount, event.paymentId());
            return FraudResult.fail("Amount exceeds maximum allowed threshold of " + maxAmount);
        }

        String accountId = decryptAccountId(event);
        long txCount = incrementAndGetVelocity(accountId);
        if (txCount > velocityMaxTransactions) {
            log.warn("FRAUD RULE 2 TRIGGERED: account {} has {} transactions in {}s paymentId={}",
                    accountId, txCount, velocityWindowSeconds, event.paymentId());
            return FraudResult.fail("Velocity limit exceeded: more than " + velocityMaxTransactions
                    + " transactions in " + velocityWindowSeconds + " seconds");
        }

        // TODO: Rule 3 - cross-border transaction check
        // TODO: Rule 4 - blacklisted account check (Redis Set lookup)
        // TODO: Rule 5 - ML fraud score threshold (call external scoring service)

        log.info("Payment passed all fraud rules paymentId={} amount={}", event.paymentId(), amount);
        return FraudResult.pass();
    }

    /**
     * Implements a Redis-based sliding window counter using INCR + EXPIRE.
     * Each account has one key per window; the key expires after the window duration.
     *
     * TODO: switch to a Redis sorted-set sliding window (ZADD + ZCOUNT + ZREMRANGEBYSCORE)
     *       for sub-second accuracy.
     */
    private long incrementAndGetVelocity(String accountId) {
        String key = VELOCITY_PREFIX + accountId;
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(velocityWindowSeconds));
        }
        return count == null ? 0 : count;
    }

    private BigDecimal decryptAmount(PaymentEvent event) {
        try {
            return new BigDecimal(AesUtil.decrypt(event.encryptedAmount(), aesKey));
        } catch (Exception e) {
            log.error("Failed to decrypt amount for paymentId={}", event.paymentId(), e);
            return BigDecimal.valueOf(Long.MAX_VALUE);
        }
    }

    private String decryptAccountId(PaymentEvent event) {
        try {
            return AesUtil.decrypt(event.encryptedAccountId(), aesKey);
        } catch (Exception e) {
            log.error("Failed to decrypt accountId for paymentId={}", event.paymentId(), e);
            return "unknown";
        }
    }

    public record FraudResult(boolean passed, String reason) {
        public static FraudResult pass() {
            return new FraudResult(true, null);
        }
        public static FraudResult fail(String reason) {
            return new FraudResult(false, reason);
        }
    }
}
