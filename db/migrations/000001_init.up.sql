CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_player_currency UNIQUE (player_id, currency)
);

CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    origin VARCHAR(20) NOT NULL,
    provider_id VARCHAR(50),
    external_id VARCHAR(100),
    payload_hash VARCHAR(255),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id UUID NOT NULL,
    kind VARCHAR(20) NOT NULL,
    amount BIGINT NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL,
    reference_id UUID,
    resolved_reference_id UUID,
    status VARCHAR(30) NOT NULL,
    failure_code VARCHAR(50),
    result_balance BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_tx_provider_external ON transactions (provider_id, external_id) WHERE provider_id IS NOT NULL AND external_id IS NOT NULL;
CREATE UNIQUE INDEX uq_tx_opening ON transactions (wallet_id) WHERE kind = 'OPENING' AND status = 'PROCESSED';
CREATE UNIQUE INDEX uq_tx_rollback ON transactions (resolved_reference_id) WHERE kind = 'ROLLBACK' AND status = 'PROCESSED';

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES transactions(id),
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_ledger_math CHECK (
        (direction = 'DEBIT' AND balance_after = balance_before - amount) OR
        (direction = 'CREDIT' AND balance_after = balance_before + amount)
    )
);

CREATE TABLE inbox_events (
    id UUID PRIMARY KEY,
    message_id VARCHAR(255) NOT NULL UNIQUE,
    hash VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_events (status, next_attempt_at) WHERE status = 'PENDING';

-- Trigger to protect ledger from updates or deletes
CREATE OR REPLACE FUNCTION trg_protect_ledger()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Ledger entries are immutable and cannot be updated or deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_ledger_update_delete
BEFORE UPDATE OR DELETE ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION trg_protect_ledger();

-- Trigger to protect terminal transactions from being updated
CREATE OR REPLACE FUNCTION trg_protect_terminal_transactions()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED') AND (OLD.status IS DISTINCT FROM NEW.status OR OLD.result_balance IS DISTINCT FROM NEW.result_balance) THEN
        RAISE EXCEPTION 'Terminal transactions cannot be modified';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_terminal_transaction_update
BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION trg_protect_terminal_transactions();

