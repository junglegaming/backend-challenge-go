CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    provider_id VARCHAR(50) NOT NULL,
    external_transaction_id VARCHAR(100) NOT NULL,
    player_id UUID NOT NULL,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    round_id VARCHAR(100),
    game_id VARCHAR(100),
    kind VARCHAR(20) NOT NULL,
    amount BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    reference_external_transaction_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    failure_code VARCHAR(50),
    idempotency_key VARCHAR(128) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(provider_id, external_transaction_id)
);

CREATE TABLE wallet_ledger (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    direction VARCHAR(10) NOT NULL,
    amount BIGINT NOT NULL,
    balance_before BIGINT NOT NULL,
    balance_after BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(wallet_id, transaction_id)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE inbox_messages (
    consumer_name VARCHAR(50) NOT NULL,
    message_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(consumer_name, message_id)
);

CREATE OR REPLACE FUNCTION prevent_update_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Updates and Deletes are not allowed on this table (append-only)';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_append_only
BEFORE UPDATE OR DELETE ON wallet_ledger
FOR EACH ROW
EXECUTE FUNCTION prevent_update_delete();
