DROP TRIGGER IF EXISTS wallet_ledger_entries_no_truncate ON wallet_ledger_entries;
DROP TRIGGER IF EXISTS wallet_ledger_entries_no_update_delete ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS forbid_ledger_mutation();
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS inbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS wager_transactions;