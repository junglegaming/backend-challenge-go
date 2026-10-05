-- Dropping the kind removes the OPENING rows first, or the CHECK cannot be
-- restored. Ledger entries that point at them are kept: the ledger has no FK
-- to wager_transactions. This is for development rollbacks only.
DELETE FROM wager_transactions WHERE kind = 'OPENING';

ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_kind_check;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_kind_check
    CHECK (kind IN ('BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'));