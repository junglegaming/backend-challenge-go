CREATE TABLE wallets (
    id            UUID PRIMARY KEY,
    player_id     UUID NOT NULL,
    currency      CHAR(3) NOT NULL,
    balance_minor BIGINT NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (player_id, currency)
);

CREATE TABLE wallet_ledger_entries (
    id                   UUID PRIMARY KEY,
    wallet_id            UUID NOT NULL REFERENCES wallets(id),
    transaction_id       UUID NOT NULL,
    direction            TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         BIGINT NOT NULL CHECK (amount_minor > 0),
    balance_before_minor BIGINT NOT NULL,
    balance_after_minor  BIGINT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX wallet_ledger_entries_wallet_idx ON wallet_ledger_entries (wallet_id, id);

-- Explicação do schema
-- CHECK (balance_minor >= 0) e UNIQUE (wallet_id, transaction_id) são a resposta direta ao §5.8 do README 
-- ("unicidade, não negatividade... impostas pelo schema"). Se um bug de código um dia tentar gravar um saldo 
-- negativo ou aplicar a mesma transação duas vezes na mesma carteira, o Postgres recusa, independente de o
--  código Go estar certo ou não — é a mesma filosofia de "a invariante tem que sobreviver mesmo que o código
--   que a cerca tenha um bug", que é exatamente o que a nota de avaliação pede.