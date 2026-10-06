ALTER TABLE transactions ADD COLUMN attempts INT NOT NULL DEFAULT 0;
ALTER TABLE transactions ADD COLUMN next_attempt_at TIMESTAMPTZ;
UPDATE transactions SET next_attempt_at = created_at WHERE status = 'PENDING_REFERENCE';
