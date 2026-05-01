# Event-Driven Payment Processing System

A production-grade, event-driven payment platform built with Java 21 and Spring Boot 3.2. Six microservices communicate asynchronously through Apache Kafka, with AES-256 field-level encryption, HMAC-SHA256 message integrity, RS256 JWT authentication, and a distributed saga for consistency.

---

## Architecture

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| **api-gateway** | 8080 | RS256 JWT validation, Redis rate limiting, request routing |
| **payment-service** | 8081 | REST intake, AES-256 encryption, idempotency, Kafka producer |
| **fraud-service** | 8082 | Amount threshold and velocity rule checks |
| **ledger-service** | 8083 | Double-entry bookkeeping, append-only PostgreSQL event store |
| **notification-service** | 8084 | Email and webhook delivery on payment state changes |
| **settlement-service** | 8085 | Saga orchestrator - triggers compensating transactions on failure |

### Dev tooling

| Tool | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:9093 | Browse topics, partitions, and messages |
| MailHog | http://localhost:8025 | Inspect all outbound notification emails |

---

## Kafka Topics

| Topic | Partitions | Producer | Consumers                                                |
|---|---|---|----------------------------------------------------------|
| `payment.initiated` | 3 | payment-service | fraud-service, settlement-service                        |
| `payment.validated` | 3 | fraud-service | ledger-service, settlement-service, notification-service |
| `payment.rejected` | 3 | fraud-service | settlement-service, notification-service                 |
| `payment.settled` | 3 | ledger-service | settlement-service, notification-service                 |
| `payment.compensate` | 3 | settlement-service | ledger-service                                           |
| `payment.dlq` | 1 | all services (error handler) | -                                                        |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.2.5, Spring Cloud 2023.0.1 |
| Messaging | Apache Kafka (Confluent 7.6.1) |
| Gateway | Spring Cloud Gateway (WebFlux / Netty) |
| Database | PostgreSQL 16, Flyway migrations |
| Cache / state | Redis 7.2 (Lettuce reactive client) |
| Encryption | AES-256-GCM (field-level), HMAC-SHA256 (message integrity) |
| Authentication | RS256 JWT - asymmetric RSA-2048 |
| TLS | PKCS12 self-signed certs, generated at Docker build time |
| Build | Maven multi-module, 3-stage Docker build |
| Runtime | Eclipse Temurin 21-jre-alpine, non-root user |

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- OpenSSL (for one-time secret generation)
- bash (Git Bash on Windows)

### 1. Generate secrets

```bash
# RSA key pair - private key signs JWTs, public key verifies them in the gateway
mkdir -p certs
openssl genrsa -out certs/private-key.pem 2048
openssl rsa -in certs/private-key.pem -pubout -out certs/public-key.pem

# Application secrets (run each line, copy output to .env)
openssl rand -base64 32   # AES_ENCRYPTION_KEY
openssl rand -base64 64   # KAFKA_HMAC_SECRET
openssl rand -base64 16   # SSL_KEYSTORE_PASSWORD
openssl rand -base64 16   # POSTGRES_PASSWORD
openssl rand -base64 16   # REDIS_PASSWORD
```

Full guide: [docs/KEY_GENERATION.md](docs/KEY_GENERATION.md)

### 2. Configure environment

```bash
cp .env.example .env
# Open .env and fill in the values from step 1
```

### 3. Build and start

```bash
docker compose build --no-cache
docker compose up -d
```

Services take 60–90 seconds to start. Watch logs:

```bash
docker compose logs -f payment-service fraud-service ledger-service
```

### 4. Test

```bash
# Infrastructure health checks + direct payment-service tests
./scripts/test-api.sh

# Generate a JWT, then run the full gateway flow test
source <(./scripts/generate-test-jwt.sh | grep 'export TOKEN')
./scripts/test-api.sh
```

What to check after a payment:
- **Kafka UI** http://localhost:9093 - watch messages move through topics
- **MailHog** http://localhost:8025 - notification emails appear here
- **Logs** `docker compose logs -f payment-service fraud-service ledger-service settlement-service`

---

## Security Model

### Authentication - RS256 JWT
All external requests require a `Bearer` token. The API gateway validates it using the RSA public key mounted at `certs/public-key.pem`. Internal services are network-isolated and do not re-validate tokens - they trust the `X-User-Id` header injected by the gateway.

### Field-level encryption - AES-256-GCM
`accountId`, `destinationAccountId`, and `amount` are encrypted by payment-service before being stored or published to Kafka. Format: `Base64(IV ‖ ciphertext ‖ GCM_tag)` with a fresh 12-byte random IV per message. Only services that need a plaintext value decrypt it.

### Message integrity - HMAC-SHA256
Every `PaymentEvent` carries a signature over all fields (excluding the signature itself, using sorted-key canonical JSON). Consumers verify before processing. Verification uses constant-time comparison to prevent timing attacks.

### Rate limiting
Redis sliding window at the API gateway: 50 requests/second sustained, 100 burst per source IP.

### TLS
All services serve HTTPS only. Keystores are generated automatically per service during `docker compose build`. The gateway uses an insecure trust manager for internal service calls (self-signed certs on a private Docker network); external traffic is verified normally by callers.

---

## Project Structure

```
.
├-- api-gateway/               Spring Cloud Gateway - JWT, rate limiting, routing
├-- payment-service/           REST API - encryption, idempotency, Kafka producer
├-- fraud-service/             Kafka consumer - rule engine, fraud detection
├-- ledger-service/            Kafka consumer - double-entry bookkeeping (PostgreSQL)
├-- notification-service/      Kafka consumer - email + webhook delivery
├-- settlement-service/        Kafka consumer/producer - saga orchestrator
├-- shared-lib/                DTOs, crypto utilities, Kafka topic constants
├-- docs/
│   ├-- DESIGN.md              Pre-development architecture and design decisions
│   └-- KEY_GENERATION.md      Secret generation and rotation guide
├-- scripts/
│   ├-- generate-test-jwt.sh   RS256 JWT generator (bash + openssl, no extra tools)
│   └-- test-api.sh            End-to-end integration test script
├-- certs/                     private-key.pem (gitignored), public-key.pem
├-- Dockerfile                 Shared multi-stage build (parameterised by SERVICE_NAME)
├-- docker-compose.yml         Full infrastructure + 6 services
└-- .env.example               Committed secrets template (fill in → .env)
```

---

## Common Operations

### Rebuild a single service

```bash
docker compose build --no-cache payment-service
docker compose up -d payment-service
```

### Follow logs

```bash
docker compose logs -f payment-service fraud-service ledger-service settlement-service
```

### Reset all state

```bash
# Drops containers AND volumes - clears Kafka offsets, PostgreSQL data, Redis
docker compose down -v
docker compose up -d
```

### Rotate the JWT key pair

```bash
openssl genrsa -out certs/private-key.pem 2048
openssl rsa -in certs/private-key.pem -pubout -out certs/public-key.pem
docker compose restart api-gateway
```

New tokens must be generated with `./scripts/generate-test-jwt.sh` after rotation.
See [docs/KEY_GENERATION.md §9](docs/KEY_GENERATION.md) for rotation procedures for all key types.
