DROP TABLE IF EXISTS outbox_events;
DROP FUNCTION IF EXISTS outbox_events_guard();
DROP TABLE IF EXISTS inbox_messages;
DROP TRIGGER IF EXISTS ledger_entries_match_wallet ON ledger_entries;
DROP FUNCTION IF EXISTS ledger_entries_match_wallet();
DROP TABLE IF EXISTS ledger_entries;
DROP FUNCTION IF EXISTS ledger_entries_chain();
DROP FUNCTION IF EXISTS ledger_entries_immutable();
DROP TABLE IF EXISTS wager_transactions;
DROP FUNCTION IF EXISTS wager_transactions_guard();
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS wallets_match_ledger();
DROP FUNCTION IF EXISTS wallets_guard();

-- The table grants went with the tables. The role is cluster-wide: it is
-- dropped only when no login user is a member and no other database still
-- grants it anything; otherwise it is kept.
DO $$
BEGIN
    IF to_regrole('wallet_app') IS NULL THEN
        RETURN;
    END IF;
    REVOKE USAGE ON SCHEMA public FROM wallet_app;
    IF EXISTS (SELECT FROM pg_auth_members WHERE roleid = 'wallet_app'::regrole) THEN
        RETURN;
    END IF;
    BEGIN
        DROP ROLE wallet_app;
    EXCEPTION WHEN dependent_objects_still_exist THEN
        RAISE NOTICE 'role wallet_app is still used by another database; kept';
    END;
END;
$$;
