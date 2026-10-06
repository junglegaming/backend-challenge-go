DROP TRIGGER IF EXISTS prevent_terminal_transaction_update ON transactions;
DROP FUNCTION IF EXISTS trg_protect_terminal_transactions;
DROP TRIGGER IF EXISTS prevent_ledger_update_delete ON ledger_entries;
DROP FUNCTION IF EXISTS trg_protect_ledger;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_events;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS wallets;
