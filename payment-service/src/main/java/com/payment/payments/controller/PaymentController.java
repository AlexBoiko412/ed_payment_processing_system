package com.payment.payments.controller;

import com.payment.payments.service.PaymentService;
import com.payment.shared.dto.PaymentRequest;
import com.payment.shared.dto.PaymentResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    /**
     * Initiates a new payment.
     *
     * The X-User-Id header is injected by the API Gateway after JWT validation -
     * it is trusted because it arrives from the gateway's internal network, not
     * from the client.
     *
     * Idempotency: if the same idempotencyKey arrives twice within the TTL window,
     * the cached response is returned with HTTP 200 instead of creating a duplicate.
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> initiatePayment(
            @Valid @RequestBody PaymentRequest request,
            @RequestHeader(value = "X-User-Id", required = false) String userId,
            @RequestHeader(value = "Idempotency-Key", required = false) String headerIdempotencyKey) {

        String idempotencyKey = headerIdempotencyKey != null
                ? headerIdempotencyKey
                : request.idempotencyKey();

        log.info("Payment initiation request received userId={} idempotencyKey={}", userId, idempotencyKey);

        PaymentResponse response = paymentService.initiatePayment(request, idempotencyKey);

        HttpStatus status = paymentService.wasDuplicate(idempotencyKey)
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED;

        return ResponseEntity.status(status).body(response);
    }

    /**
     * Retrieves the current status of a payment by its payment ID.
     * TODO: implement - query a status store (Redis or DB) populated by the saga.
     */
    @GetMapping("/{paymentId}")
    public ResponseEntity<PaymentResponse> getPaymentStatus(@PathVariable String paymentId) {
        // TODO: look up payment status from Redis/DB
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
    }
}
