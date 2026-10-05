DROP TRIGGER IF EXISTS wallet_ledger_append_only ON wallet_ledger;
DROP FUNCTION IF EXISTS prevent_update_delete();

DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS wallet_ledger;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
