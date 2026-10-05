CREATE TABLE wager_transactions (
    id                              UUID PRIMARY KEY,
    provider_id                     TEXT NOT NULL,
    external_transaction_id         TEXT NOT NULL,
    idempotency_key                 TEXT NOT NULL,
    request_hash                    TEXT NOT NULL,
    wallet_id                       UUID NOT NULL REFERENCES wallets(id),
    player_id                       UUID NOT NULL,
    round_id                        TEXT NOT NULL,
    game_id                         TEXT NOT NULL,
    kind                            TEXT NOT NULL CHECK (kind IN ('BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor                    BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency                        CHAR(3) NOT NULL,
    reference_external_transaction_id TEXT,
    reference_transaction_id        UUID REFERENCES wager_transactions(id),
    state                           TEXT NOT NULL CHECK (state IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code                    TEXT,
    balance_after_minor             BIGINT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- LOSS moves no money and must be zero; every other kind must move a positive amount.
    CHECK ((kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),
    UNIQUE (provider_id, external_transaction_id)
);

-- A referenced operation can be reversed at most once: at most one REFUND or
-- ROLLBACK may be PROCESSED against the same transaction. The database
-- enforces it, so a bug in application code cannot double-return a bet.
CREATE UNIQUE INDEX wager_one_successful_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND state = 'PROCESSED';

CREATE TABLE idempotency_keys (
    scope                TEXT NOT NULL,
    endpoint             TEXT NOT NULL,
    key                  TEXT NOT NULL,
    request_hash         TEXT NOT NULL,
    state                TEXT NOT NULL CHECK (state IN ('in_flight', 'completed')),
    status_code          INT,
    response_body        TEXT,
    wager_transaction_id UUID REFERENCES wager_transactions(id),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, endpoint, key),
    -- A completed key must carry the response it will replay.
    CHECK (state = 'in_flight' OR (status_code IS NOT NULL AND response_body IS NOT NULL))
);

-- Message-level deduplication for SQS: one row per (consumer, message).
CREATE TABLE inbox (
    consumer_name TEXT NOT NULL,
    message_id    TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);

-- Events are written in the same transaction as the state they describe.
-- A publisher (not built yet) will drain rows where published_at IS NULL.
CREATE TABLE outbox (
    id              UUID PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX outbox_pending_idx ON outbox (next_attempt_at) WHERE published_at IS NULL;

-- The ledger is append-only (README §5.5). Corrections are new entries, never
-- edits, so UPDATE, DELETE and TRUNCATE are refused by the database itself.
CREATE OR REPLACE FUNCTION forbid_ledger_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();

CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();