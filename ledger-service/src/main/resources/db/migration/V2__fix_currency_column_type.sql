-- Fixes CHAR(3) → VARCHAR(3) for the currency column.
-- CHAR(3) is stored as bpchar in PostgreSQL; Hibernate expects varchar and
-- fails schema validation on startup. This migration corrects existing databases
-- that already ran V1 with the wrong type.
ALTER TABLE ledger_events
    ALTER COLUMN currency TYPE VARCHAR(3);
