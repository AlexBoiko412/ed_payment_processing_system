-- Ledger Event Store - append-only event sourcing table

CREATE TABLE ledger_events
(
    id                          UUID        NOT NULL DEFAULT gen_random_uuid(),
    payment_id                  VARCHAR(36) NOT NULL,
    event_type                  VARCHAR(20) NOT NULL,   -- DEBIT | CREDIT | COMPENSATE

    -- AES-256-GCM encrypted fields (Base64-encoded: IV || ciphertext+tag)
    encrypted_account_id            TEXT        NOT NULL,
    encrypted_destination_account_id TEXT       NOT NULL,
    encrypted_amount                TEXT        NOT NULL,
    encrypted_balance_after         TEXT,               -- NULL until materialised view is built

    currency                    VARCHAR(3)  NOT NULL,
    idempotency_key             VARCHAR(36) NOT NULL,

    -- HMAC-SHA256 of all other columns (hex-encoded) for tamper detection
    signature                   TEXT        NOT NULL,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ledger_events_pkey         PRIMARY KEY (id),
    -- Ensures exactly one ledger entry per payment per event type
    CONSTRAINT ledger_events_payment_type UNIQUE (payment_id, event_type),
    CONSTRAINT ledger_events_event_type_check
        CHECK (event_type IN ('DEBIT', 'CREDIT', 'COMPENSATE'))
);

CREATE INDEX idx_ledger_account_time
    ON ledger_events (encrypted_account_id, created_at);

CREATE INDEX idx_ledger_payment_id
    ON ledger_events (payment_id);

CREATE UNIQUE INDEX idx_ledger_idempotency_key
    ON ledger_events (idempotency_key, event_type);

COMMENT ON TABLE ledger_events IS
    'Append-only event store for double-entry bookkeeping. '
    'Never UPDATE or DELETE rows - compensating entries use COMPENSATE event_type.';
