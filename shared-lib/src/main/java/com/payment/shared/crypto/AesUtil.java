package com.payment.shared.crypto;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * AES-256-GCM encryption utility for sensitive fields at rest.
 *
 * Scheme: AES/GCM/NoPadding with a 96-bit (12-byte) random IV prepended to
 * the ciphertext. The combined bytes are Base64-encoded for storage.
 *
 *   stored value = Base64( IV(12 bytes) || Ciphertext+AuthTag )
 *
 * The 128-bit GCM authentication tag provides integrity and authenticity -
 * any tampering with the ciphertext will cause decryption to throw.
 *
 * Key material: a Base64-encoded 256-bit (32-byte) random key loaded from the
 * {@code AES_ENCRYPTION_KEY} environment variable. Generate with:
 *   openssl rand -base64 32
 * or call {@link #generateKeyBase64()}.
 *
 * TODO: integrate with a KMS (AWS KMS / HashiCorp Vault) for envelope encryption
 *       in production so the raw key never lives in env vars.
 */
public final class AesUtil {

    private static final String ALGORITHM      = "AES/GCM/NoPadding";
    private static final int    GCM_IV_LENGTH  = 12;
    private static final int    GCM_TAG_BITS   = 128;

    private AesUtil() {}

    /**
     * Encrypts {@code plaintext} with the given Base64-encoded AES-256 key.
     *
     * @param plaintext    the sensitive value to encrypt (e.g. account number)
     * @param base64Key    Base64-encoded 32-byte AES key
     * @return Base64-encoded string: IV || ciphertext+tag
     */
    public static String encrypt(String plaintext, String base64Key) {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(base64Key);
            SecretKey key   = new SecretKeySpec(keyBytes, "AES");

            byte[] iv = new byte[GCM_IV_LENGTH];
            SecureRandom.getInstanceStrong().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(UTF_8));

            // Prepend IV so decrypt() can extract it without a separate storage column
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);

            return Base64.getEncoder().encodeToString(combined);
        } catch (Exception e) {
            throw new EncryptionException("AES-256-GCM encryption failed", e);
        }
    }

    /**
     * Decrypts a value produced by {@link #encrypt}.
     *
     * @param ciphertext  Base64-encoded string: IV || ciphertext+tag
     * @param base64Key   Base64-encoded 32-byte AES key
     * @return original plaintext string
     * @throws EncryptionException if decryption fails (bad key, tampered data, etc.)
     */
    public static String decrypt(String ciphertext, String base64Key) {
        try {
            byte[] keyBytes  = Base64.getDecoder().decode(base64Key);
            SecretKey key    = new SecretKeySpec(keyBytes, "AES");
            byte[] combined  = Base64.getDecoder().decode(ciphertext);

            byte[] iv        = Arrays.copyOfRange(combined, 0, GCM_IV_LENGTH);
            byte[] encrypted = Arrays.copyOfRange(combined, GCM_IV_LENGTH, combined.length);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));

            return new String(cipher.doFinal(encrypted), UTF_8);
        } catch (Exception e) {
            throw new EncryptionException("AES-256-GCM decryption failed", e);
        }
    }

    /**
     * Generates a new random AES-256 key and returns it Base64-encoded.
     * Run once during initial setup; store the result in a secret manager.
     */
    public static String generateKeyBase64() {
        try {
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            kg.init(256, SecureRandom.getInstanceStrong());
            SecretKey key = kg.generateKey();
            return Base64.getEncoder().encodeToString(key.getEncoded());
        } catch (Exception e) {
            throw new EncryptionException("Failed to generate AES-256 key", e);
        }
    }

    public static final class EncryptionException extends RuntimeException {
        public EncryptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
