# System Design - Event-Driven Payment Processing Platform

**Status:** Pre-development design  
**Stack:** Java 21 · Spring Boot 3.2 · Apache Kafka · PostgreSQL · Redis

---

## 1. Problem Statement

We need a payment processing backend that is:

- **Reliable under concurrent load** - the same payment must not be processed twice, even if the client retries or a network failure occurs mid-request
- **Auditable** - every state change in a payment's lifecycle must be traceable and immutable
- **Secure by default** - sensitive financial data (account numbers, amounts) must not appear in plaintext in logs, message brokers, or databases
- **Resilient to partial failure** - if one processing step fails after others have succeeded, the system must self-correct without requiring manual intervention
- **Observable** - developers and operators must be able to see what is happening inside the system in real time

A monolithic approach is ruled out at the design stage because the fraud detection, ledger accounting, settlement, and notification concerns have different scalability profiles, different failure modes, and would create tight coupling that makes independent iteration difficult.

---

## 2. Goals and Non-Goals

### Goals

- Accept payment requests over a secure REST API
- Detect potentially fraudulent payments using configurable rules before committing them
- Record every settled payment as an immutable double-entry ledger event
- Notify clients via email and webhook on payment status changes
- Ensure consistency across services even when individual services fail - using a saga pattern with compensating transactions
- Protect sensitive field values (account IDs, amounts) in transit and at rest using encryption
- Authenticate API callers using asymmetric JWTs - without requiring a separate identity server in the initial version
- Be fully runnable on a single developer machine with a single `docker compose up`

### Non-Goals (initial version)

- Real-time payment settlement with external banking systems (ACH, SWIFT)
- Multi-tenant support or customer account management
- A production-ready identity provider (OAuth2 authorization server, SSO)
- GDPR data deletion workflows
- Horizontal Kafka cluster scaling (single broker sufficient for dev)
- Payment card data (PCI-DSS scope is out)

---

## 3. Architecture Overview

The system is decomposed into six microservices that communicate exclusively through Kafka topics. No service calls another service directly (except the API gateway forwarding the initial REST request to payment-service). This decoupling means each service can be deployed, scaled, and failed independently.

<img width="2456" height="2299" alt="payment1" src="https://github.com/user-attachments/assets/884f5854-0c3e-414c-8f47-f05742dd336e" />


---

## 4. Design Decisions

### 4.1 Kafka for inter-service communication

**Decision:** All inter-service communication uses Kafka topics rather than synchronous HTTP calls.

**Rationale:**
- A payment takes a non-trivial amount of time to process (fraud check, ledger write). Making the client wait synchronously for all of this would increase latency and couple the client's availability to every internal service.
- Kafka gives us replay: if fraud-service is restarted or crashes, it re-reads from its committed offset and processes messages it missed. No messages are lost.
- Kafka's consumer group model lets us add additional consumers (e.g. analytics, audit) to any topic without touching producers.
- The ordered-per-partition guarantee means all events for a given `paymentId` are processed in the order they were written.

**Trade-off:** Eventual consistency. The API returns 202 Accepted immediately; the client must poll or listen for a webhook to learn the final outcome. This is the correct model for payment processing - most real payment systems work the same way.

### 4.2 Event sourcing in the ledger

**Decision:** The ledger service writes every balance change as an immutable row in an append-only `ledger_events` table. Balances are derived by replaying events. The table is never updated or deleted.

**Rationale:**
- Financial regulations typically require a full audit trail. Append-only is the simplest way to guarantee it - there is no `UPDATE` or `DELETE` code path to accidentally corrupt history.
- Event sourcing makes compensating transactions natural: a reversal is just another event (type `COMPENSATE`) appended to the same table rather than a row being mutated.
- Replaying events from the beginning gives us the current balance at any point in time and makes historical balance queries (balance as of date X) straightforward.

**Trade-off:** Balance queries are O(n) on the number of events for an account. For production scale this would require a materialized balance view or snapshot table updated by a background job. For this version, replay is acceptable.

### 4.3 Saga pattern for distributed consistency

**Decision:** Use the orchestration variant of the saga pattern, with settlement-service as the coordinator. If any step fails after payment initiation, settlement-service emits a `payment.compensate` event and ledger-service writes reversal entries.

**Rationale:**
- Distributed transactions (2-phase commit) across Kafka consumers and PostgreSQL would require XA transactions, adding significant operational complexity and becoming a bottleneck.
- The saga pattern achieves eventual consistency without a distributed lock. Each service only needs to be transactionally consistent within its own database.
- The orchestration variant (one coordinator service) is easier to reason about and debug than choreography (where each service decides what to do next based on the event it received).

**Trade-off:** Compensating transactions require careful design - they must be idempotent and must never fail themselves. A failed compensation is a hard problem that would require manual intervention or an additional retry saga. This is acceptable for an initial design.

### 4.4 Field-level encryption over transport-layer-only encryption

**Decision:** Encrypt `accountId`, `destinationAccountId`, and `amount` at the application layer (AES-256-GCM) before publishing them to Kafka. TLS protects the transport; AES protects the data at rest inside Kafka and the database.

**Rationale:**
- TLS alone protects data in transit but not at rest. Any service or operator with access to the Kafka broker or PostgreSQL database would see plaintext PII without application-layer encryption.
- AES-256-GCM is authenticated encryption - the GCM tag detects any tampering with the ciphertext.
- Prepending a fresh random IV per message to the ciphertext ensures that encrypting the same value twice produces different output, preventing frequency analysis.
- Only the services that need a plaintext value (fraud-service for amount, ledger-service for both) receive the decryption key.

**Trade-off:** Every service that needs a plaintext value must hold the `AES_ENCRYPTION_KEY`. In production this key would come from a secrets manager (Vault, AWS KMS) rather than an environment variable.

### 4.5 HMAC-SHA256 Kafka message signing

**Decision:** Every `PaymentEvent` is signed with HMAC-SHA256 before being published. Consumers verify the signature before processing.

**Rationale:**
- Kafka itself has no built-in message integrity guarantees at the application level. A compromised producer or a man-in-the-middle attack (if Kafka mTLS is misconfigured) could inject forged events.
- HMAC is cheap to compute and verify, adds a fixed small overhead per message, and does not require asymmetric key infrastructure.
- Constant-time comparison on the consumer side prevents timing-based attacks that could be used to forge signatures byte-by-byte.

**Trade-off:** All services must share the same HMAC secret. Key rotation requires restarting all services simultaneously. In production this would be managed by a secrets manager with versioned secrets and a rolling rotation strategy.

### 4.6 RS256 JWT at the gateway - no auth server required

**Decision:** The API gateway validates JWTs using a static RSA public key loaded from a volume-mounted PEM file. There is no OAuth2 authorization server in the initial version.

**Rationale:**
- An authorization server (Keycloak, Auth0, Okta) adds significant operational complexity. The goal for this version is to demonstrate the payment pipeline - authentication is a supporting concern.
- RS256 (asymmetric) is preferred over HS256 (symmetric) because the gateway only needs the public key to validate tokens. The signing key never needs to be distributed to the gateway.
- The JWT issuer field (`payment-system-dev`) and audience (`payment-api`) are validated by the gateway to prevent token reuse from other systems.

**Trade-off:** Token revocation is not possible without additional infrastructure (a revocation list or short expiry + refresh token flow). Tokens are valid for their full 1-hour TTL once issued. This is acceptable for the initial version.

### 4.7 Idempotency via Redis

**Decision:** Payment-service stores each idempotency key in Redis with a 24-hour TTL. On receipt of a duplicate key, it returns the original response (200 OK + cached paymentId) without re-processing.

**Rationale:**
- Network failures frequently cause clients to retry requests. Without idempotency, a retry could charge a customer twice.
- Redis provides O(1) key existence checks and automatic expiry, making it ideal for short-lived idempotency windows.
- The 24-hour TTL balances correctness (duplicates within a day are rejected) and storage (keys expire automatically).

**Trade-off:** If Redis is unavailable, the idempotency check fails open (the request proceeds). In production, the service should fail closed or implement a fallback to a database-backed idempotency store.

---

## 5. Service Contracts

### 5.1 REST API (payment-service via api-gateway)

```
POST /api/payments
Authorization: Bearer <RS256 JWT>
Idempotency-Key: <UUID v4>           (optional; auto-generated if absent)
Content-Type: application/json

{
  "accountId": "ACC-001",
  "destinationAccountId": "ACC-002",
  "amount": 100.00,
  "currency": "USD",
  "metadata": {}                     (optional key/value pass-through)
}

Responses:
202 Accepted   { "paymentId": "<uuid>", "status": "INITIATED" }
200 OK         { "paymentId": "<uuid>", "status": "INITIATED" }  (duplicate idempotency key)
400 Bad Request  (validation failure - missing required field, invalid amount)
401 Unauthorized (missing or invalid JWT)
429 Too Many Requests (rate limit exceeded)
```

### 5.2 Kafka event schema (PaymentEvent)

All Kafka messages share a single envelope type. Fields that are not relevant to a given lifecycle stage are null.

```json
{
  "eventId": "uuid-v4",
  "paymentId": "uuid-v4",
  "encryptedAccountId": "<AES-256-GCM base64>",
  "encryptedDestinationAccountId": "<AES-256-GCM base64>",
  "encryptedAmount": "<AES-256-GCM base64>",
  "currency": "USD",
  "status": "INITIATED | VALIDATED | REJECTED | SETTLED | COMPENSATED",
  "idempotencyKey": "<string>",
  "timestamp": "<epoch seconds>",
  "signature": "<HMAC-SHA256 base64>",
  "rejectionReason": "<string | null>",
  "metadata": {}
}
```

---

## 6. Data Model

### 6.1 Ledger events (PostgreSQL - append-only)

```sql
CREATE TABLE ledger_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id    UUID        NOT NULL,
    event_type    VARCHAR(20) NOT NULL,  -- DEBIT, CREDIT, COMPENSATE
    account_id    TEXT        NOT NULL,  -- AES-256-GCM encrypted
    amount        TEXT        NOT NULL,  -- AES-256-GCM encrypted
    currency      VARCHAR(3)  NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    idempotency_key VARCHAR(255),
    metadata      TEXT
);
```

Every settled payment produces exactly two rows: a DEBIT on the source account and a CREDIT on the destination account, written atomically in a single transaction.

### 6.2 Redis key space

| Key pattern | Value | TTL | Owner |
|---|---|---|---|
| `idem:<idempotencyKey>` | `<paymentId>` | 24 hours | payment-service |
| `rate:<ip>` | sliding window counter | managed by Redis rate limiter | api-gateway |
| `velocity:<accountId>` | transaction count | 60 seconds | fraud-service |
| `saga:<paymentId>` | saga state JSON | configurable (default 24h) | settlement-service |

---

## 7. Fraud Rules

The initial rule set is intentionally minimal and configurable via environment variables. Rules are applied in order; the first failure short-circuits further checks.

| Rule | Parameter | Default | Description |
|---|---|---|---|
| Amount threshold | `fraud.rules.max-amount` | 10,000 | Reject if decrypted amount exceeds this value |
| Transaction velocity | `fraud.rules.velocity-window-seconds` | 60 | Sliding window duration |
| Transaction velocity | `fraud.rules.velocity-max-transactions` | 5 | Max transactions per account in the window |

The rule engine is designed to be extended: each rule is an independent check that returns a `FraudResult` (passed / reason). Adding a new rule requires implementing the check and registering it - no changes to consumers or producers are needed.

---

## 8. Security Boundaries

<img width="2867" height="1806" alt="payment2" src="https://github.com/user-attachments/assets/e33bfde2-e739-40e1-b848-b9f946dff8e1" />


In production:
- Kafka should use mTLS with per-service client certificates
- Services should be deployed in a private VPC/subnet with network policies preventing direct internet access
- The `AES_ENCRYPTION_KEY` and `KAFKA_HMAC_SECRET` should come from a secrets manager, not environment variables
- The RSA private key should be held only by the identity provider, not on the filesystem

---

## 9. Failure Modes and Recovery

| Failure | Behaviour | Recovery |
|---|---|---|
| payment-service crash after Kafka publish | Event already in Kafka; service restarts with `restart: on-failure:3`; no duplicate (idempotency key in Redis) | Automatic |
| fraud-service crash mid-processing | Kafka offset not committed (manual ack mode); event re-delivered on restart | Automatic |
| ledger-service DB connection lost | Consumer pauses; Kafka retains the message; DB reconnects | Automatic |
| Ledger write fails after 3 retries | Message routed to `payment.dlq` by `DeadLetterPublishingRecoverer` | Manual inspection of DLQ |
| Settlement determines payment cannot be completed | Emits `payment.compensate`; ledger writes reversal entries | Automatic (saga) |
| Redis unavailable | Idempotency check skipped (fail-open); rate limiting degrades | Partial - acceptable in dev, should fail-closed in prod |

---

## 10. Observability

Every service exposes Spring Boot Actuator endpoints:
- `/actuator/health` - used by Docker Compose health checks and monitoring
- `/actuator/metrics` - Micrometer metrics (JVM, Kafka consumer lag, HTTP latency)
- `/actuator/info` - service version metadata

In development:
- **Kafka UI** (port 9093) provides a real-time view of topic contents, consumer group offsets, and message payloads
- **MailHog** (port 8025) captures all outbound notification emails without requiring a real SMTP server
- Structured JSON logging at `DEBUG` level for all `com.payment.*` packages

In production, these would feed into a log aggregation stack (ELK, Loki) and a metrics platform (Prometheus + Grafana), with alerts on consumer lag, DLQ message count, and error rate.

---

## 11. Technology Selection Rationale

| Choice | Alternatives considered | Why this |
|---|---|---|
| Apache Kafka | RabbitMQ, AWS SQS, Redis Streams | Log retention, consumer group replay, partition-level ordering, ecosystem maturity |
| Spring Cloud Gateway | NGINX, Envoy, Kong | Native Spring integration, reactive WebFlux, programmatic route definition, Redis rate limiter built-in |
| PostgreSQL for ledger | MySQL, MongoDB, Cassandra | ACID transactions for double-entry atomicity; `gen_random_uuid()` built-in; Flyway ecosystem |
| Redis for idempotency and saga state | Database, Memcached | Sub-millisecond lookup; built-in TTL; atomic INCR for velocity counters; Lua scripting for future use |
| AES-256-GCM | AES-CBC, ChaCha20-Poly1305 | Authenticated encryption (integrity + confidentiality in one operation); JVM standard library |
| HMAC-SHA256 for Kafka | Digital signatures (RSA/ECDSA) | Symmetric verification is orders of magnitude faster; sufficient for shared-secret model |
| Flyway | Liquibase | SQL-first migrations; simpler mental model; strong Spring Boot integration |
| Maven multi-module | Gradle, separate repos | Single build, shared dependency management, local shared-lib without a registry |
