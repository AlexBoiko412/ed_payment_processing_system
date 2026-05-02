CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- TODO: in production, create a limited-privilege role:
--   CREATE ROLE payment_app LOGIN PASSWORD '...';
--   GRANT CONNECT ON DATABASE payment_db TO payment_app;
--   GRANT USAGE ON SCHEMA public TO payment_app;
--   GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO payment_app;
--   -- Note: no UPDATE or DELETE - the ledger is append-only
