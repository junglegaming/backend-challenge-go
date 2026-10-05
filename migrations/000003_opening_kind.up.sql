-- OPENING is the internal operation that records the first credit of a wallet
-- opened with a positive balance. It never arrives from HTTP or SQS; the use
-- case rejects it at those edges. The table only has to accept it.
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_kind_check;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_kind_check
    CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'));