package com.payment.shared.crypto;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * HMAC-SHA256 payload signing utility.
 *
 * Usage pattern (producer side):
 * <pre>
 *   String payload  = objectMapper.writeValueAsString(eventWithoutSignature);
 *   String signature = HmacUtil.sign(payload, hmacSecret);
 *   PaymentEvent signed = event.toBuilder().signature(signature).build();
 * </pre>
 *
 * Usage pattern (consumer side):
 * <pre>
 *   String payload = objectMapper.writeValueAsString(eventWithoutSignature);
 *   if (!HmacUtil.verify(payload, event.signature(), hmacSecret)) {
 *       throw new SecurityException("Signature verification failed");
 *   }
 * </pre>
 *
 * The {@code hmacSecret} is loaded from the {@code KAFKA_HMAC_SECRET} environment
 * variable (Base64-encoded) and shared across all services.
 */
public final class HmacUtil {

    private static final String ALGORITHM = "HmacSHA256";

    private HmacUtil() {}

    /**
     * Computes a Base64-encoded HMAC-SHA256 signature of {@code payload}.
     *
     * @param payload   canonical JSON string to sign (must NOT include the signature field)
     * @param secretKey raw HMAC secret (plain UTF-8 string or Base64-decoded bytes as string)
     * @return Base64-encoded signature
     * @throws SigningException if the JVM doesn't support HmacSHA256 (won't happen on Java 21)
     */
    public static String sign(String payload, String secretKey) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(
                    secretKey.getBytes(StandardCharsets.UTF_8), ALGORITHM);
            mac.init(keySpec);
            byte[] hmacBytes = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hmacBytes);
        } catch (Exception e) {
            throw new SigningException("Failed to compute HMAC-SHA256 signature", e);
        }
    }

    /**
     * Verifies that {@code signature} matches the HMAC-SHA256 of {@code payload}.
     * Uses a constant-time comparison to prevent timing attacks.
     *
     * @return {@code true} if the signature is valid
     */
    public static boolean verify(String payload, String signature, String secretKey) {
        try {
            String expected = sign(payload, secretKey);
            return MessageDigest.isEqual(
                    Base64.getDecoder().decode(signature),
                    Base64.getDecoder().decode(expected));
        } catch (Exception e) {
            return false;
        }
    }

    public static final class SigningException extends RuntimeException {
        public SigningException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
