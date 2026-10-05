-- A strictly increasing order for outbox rows.
--
-- occurred_at cannot give it: Postgres keeps microseconds, and two events built
-- inside one transaction can share the same microsecond. The publisher has to
-- send WagerTransactionProcessed before WalletBalanceChanged, so it orders by
-- this column instead.
--
-- Per wallet the order is also the commit order: the wallet row stays locked
-- from the moment the events are inserted until the commit, so the next
-- transaction on that wallet gets a larger number only after this one is done.
ALTER TABLE outbox ADD COLUMN seq BIGSERIAL;

CREATE UNIQUE INDEX outbox_seq_key ON outbox (seq);
