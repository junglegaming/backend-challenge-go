-- Wallets: one per (player, currency); the balance can never be negative.
CREATE TABLE wallets (
    id            UUID PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT      NOT NULL CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- Wager transactions: external provider operations and the internal OPENING.
CREATE TABLE wager_transactions (
    id                                UUID PRIMARY KEY,
    origin                            TEXT        NOT NULL CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    kind                              TEXT        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            TEXT        NOT NULL CHECK (status IN ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    amount_minor                      BIGINT      NOT NULL,
    currency                          CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT CHECK (reference_external_transaction_id IS NULL OR reference_external_transaction_id <> ''),
    reference_transaction_id          UUID REFERENCES wager_transactions (id),
    failure_code                      TEXT,
    result_balance_minor              BIGINT CHECK (result_balance_minor >= 0),
    result_currency                   CHAR(3),
    attempts                          INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   TIMESTAMPTZ,
    correlation_id                    TEXT        NOT NULL DEFAULT '',
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    -- Internal and external operations have different shapes.
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Zero-value policy: LOSS is exactly zero, everything else is positive.
    CONSTRAINT wager_transactions_zero_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_transactions_reversal_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_failure_code CHECK (
        status NOT IN ('REJECTED', 'FAILED') OR failure_code IS NOT NULL
    ),
    CONSTRAINT wager_transactions_processed_result CHECK (
        status <> 'PROCESSED' OR result_balance_minor IS NOT NULL
    ),
    CONSTRAINT wager_transactions_result_currency CHECK (
        (result_balance_minor IS NULL) = (result_currency IS NULL)
        AND (result_currency IS NULL OR result_currency ~ '^[A-Z]{3}$')
    ),
    CONSTRAINT wager_transactions_pending_schedule CHECK (
        status <> 'PENDING_REFERENCE' OR next_attempt_at IS NOT NULL
    ),
    -- Target of the ledger foreign key (see ledger_entries_transaction_fkey).
    CONSTRAINT wager_transactions_ledger_target UNIQUE (id, wallet_id, currency, amount_minor)
);

-- A wallet has at most one opening credit.
CREATE UNIQUE INDEX wager_transactions_opening_key
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- (providerId, externalTransactionId) identifies one financial operation.
CREATE UNIQUE INDEX wager_transactions_external_key
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
-- Idempotency keys are scoped by provider.
CREATE UNIQUE INDEX wager_transactions_idempotency_key
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
-- A transaction can be successfully reversed (REFUND or ROLLBACK) only once.
CREATE UNIQUE INDEX wager_transactions_single_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
CREATE INDEX wager_transactions_due_pending
    ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_transactions_waiting_reference
    ON wager_transactions (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE';

-- Terminal transactions never change; identity and payload never change.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager transaction % cannot be deleted', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.amount_minor, NEW.currency, NEW.created_at)
        IS DISTINCT FROM (OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.amount_minor, OLD.currency, OLD.created_at)
        OR (NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash, NEW.round_id, NEW.game_id,
            NEW.reference_external_transaction_id)
        IS DISTINCT FROM (OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash, OLD.round_id,
            OLD.game_id, OLD.reference_external_transaction_id) THEN
        RAISE EXCEPTION 'immutable columns of wager transaction % cannot change', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

-- Append-only ledger. seq gives a stable order for cursor pagination.
CREATE TABLE ledger_entries (
    seq                  BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id                   UUID PRIMARY KEY,
    wallet_id            UUID        NOT NULL REFERENCES wallets (id),
    transaction_id       UUID        NOT NULL,
    direction            TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency             CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor BIGINT      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  BIGINT      NOT NULL CHECK (balance_after_minor >= 0),
    created_at           TIMESTAMPTZ NOT NULL,
    -- A transaction moves money at most once, and only in its own wallet,
    -- currency and amount.
    CONSTRAINT ledger_entries_transaction_key UNIQUE (transaction_id),
    CONSTRAINT ledger_entries_transaction_fkey FOREIGN KEY (transaction_id, wallet_id, currency, amount_minor)
        REFERENCES wager_transactions (id, wallet_id, currency, amount_minor),
    CONSTRAINT ledger_entries_balance_math CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

CREATE INDEX ledger_entries_wallet_seq ON ledger_entries (wallet_id, seq);

CREATE FUNCTION ledger_entries_immutable() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'ledger entries are append-only (% rejected)', TG_OP USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_immutable();

CREATE TRIGGER ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_immutable();

-- Every ledger entry must chain from the wallet balance it moves.
CREATE FUNCTION ledger_entries_chain() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    previous BIGINT;
    wallet_currency CHAR(3);
BEGIN
    SELECT currency INTO wallet_currency FROM wallets WHERE id = NEW.wallet_id;
    IF wallet_currency IS DISTINCT FROM NEW.currency THEN
        RAISE EXCEPTION 'ledger entry currency % differs from wallet currency %', NEW.currency, wallet_currency
            USING ERRCODE = 'check_violation';
    END IF;
    SELECT balance_after_minor INTO previous FROM ledger_entries
    WHERE wallet_id = NEW.wallet_id ORDER BY seq DESC LIMIT 1;
    IF COALESCE(previous, 0) <> NEW.balance_before_minor THEN
        RAISE EXCEPTION 'ledger entry for wallet % starts at % but the ledger is at %',
            NEW.wallet_id, NEW.balance_before_minor, COALESCE(previous, 0) USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_entries_chain
    BEFORE INSERT ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_chain();

-- Wallet identity is immutable and the version moves only with the balance.
CREATE FUNCTION wallets_guard() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallet % cannot be deleted', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at) IS DISTINCT FROM (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'immutable columns of wallet % cannot change', OLD.id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1)
        OR (NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version) THEN
        RAISE EXCEPTION 'wallet % version must advance by one exactly when the balance changes', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_guard
    BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();

-- At commit time the stored balance must equal the last ledger balance, so a
-- balance can never change without its ledger entry in the same transaction.
CREATE FUNCTION wallets_match_ledger() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    stored BIGINT;
    last_after BIGINT;
BEGIN
    SELECT balance_minor INTO stored FROM wallets WHERE id = NEW.id;
    SELECT balance_after_minor INTO last_after FROM ledger_entries
    WHERE wallet_id = NEW.id ORDER BY seq DESC LIMIT 1;
    IF COALESCE(last_after, 0) <> stored THEN
        RAISE EXCEPTION 'wallet % balance % does not match ledger balance %', NEW.id, stored, COALESCE(last_after, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_match_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_match_ledger();

-- A ledger entry must have the matching wallet balance by commit time.
CREATE FUNCTION ledger_entries_match_wallet() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    stored BIGINT;
    last_after BIGINT;
BEGIN
    SELECT balance_minor INTO stored FROM wallets WHERE id = NEW.wallet_id;
    SELECT balance_after_minor INTO last_after FROM ledger_entries
    WHERE wallet_id = NEW.wallet_id ORDER BY seq DESC LIMIT 1;
    IF stored IS DISTINCT FROM last_after THEN
        RAISE EXCEPTION 'ledger of wallet % ends at % but the stored balance is %', NEW.wallet_id, last_after, stored
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_entries_match_wallet
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_match_wallet();

-- Inbox: durable deduplication of consumed messages.
CREATE TABLE inbox_messages (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    payload_hash   TEXT        NOT NULL,
    transaction_id UUID REFERENCES wager_transactions (id),
    received_at    TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);

-- Transactional outbox. payload is an immutable JSON snapshot (json keeps the
-- exact text, unlike jsonb).
CREATE TABLE outbox_events (
    seq             BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id        UUID PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    partition_key   TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSON        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    claim_id        UUID,
    published_at    TIMESTAMPTZ,
    last_error      TEXT,
    dead_lettered_at TIMESTAMPTZ,
    CONSTRAINT outbox_events_single_outcome CHECK (published_at IS NULL OR dead_lettered_at IS NULL),
    CONSTRAINT outbox_events_lease_triplet CHECK (
        (locked_by IS NULL) = (locked_until IS NULL)
        AND (locked_by IS NULL) = (claim_id IS NULL)
    )
);

CREATE INDEX outbox_events_unpublished ON outbox_events (next_attempt_at, seq)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
CREATE INDEX outbox_events_partition_unpublished ON outbox_events (partition_key, seq)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

CREATE FUNCTION outbox_events_guard() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox event % cannot be deleted', OLD.event_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (NEW.event_id, NEW.aggregate_type, NEW.aggregate_id, NEW.partition_key, NEW.event_type, NEW.payload::text, NEW.occurred_at)
        IS DISTINCT FROM
       (OLD.event_id, OLD.aggregate_type, OLD.aggregate_id, OLD.partition_key, OLD.event_type, OLD.payload::text, OLD.occurred_at) THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.event_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % was already published', OLD.event_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_guard
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();

-- Runtime role. The migrator owns the schema; the service connects as a LOGIN
-- user that is a member of this NOLOGIN group and is created per environment
-- (deploy/postgres/init, test/testenv), so no password lives here. Without
-- ownership or superuser it cannot disable triggers or change
-- session_replication_role, and it has no UPDATE, DELETE or TRUNCATE on the
-- ledger. Roles are cluster-wide: an existing role is reused.
DO $$
BEGIN
    CREATE ROLE wallet_app NOLOGIN;
EXCEPTION WHEN duplicate_object OR unique_violation THEN
    NULL;
END;
$$;

GRANT USAGE ON SCHEMA public TO wallet_app;
GRANT SELECT, INSERT ON ledger_entries TO wallet_app;
GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO wallet_app;
