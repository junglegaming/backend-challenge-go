# Outbox failure and recovery policy

The relay retries transport failures, timeouts, unavailable queues, authorization
errors and unclassified errors indefinitely, using capped exponential backoff.
Repairing infrastructure or configuration restores delivery of the same event ID.
An unavailable partition head blocks later events of that wallet; other wallets
remain eligible. This favors recoverability and order over skipping committed events.

Only the SQS SDK's explicit `InvalidMessageContents` error marks an invalid payload
as permanent. After `OUTBOX_MAX_ATTEMPTS`, it is quarantined in PostgreSQL with
`dead_lettered_at` and `last_error`; its payload and event ID remain immutable.
See [AWS SendMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html).
Other error codes are conservatively retried because a configuration repair may
make the exact same event deliverable. No HTTP administration API is required.

## Inspect and recover an existing quarantine

Use a separately authorized operator connection to the intended database. Inspect
the selected row and determine the cause before changing its delivery state:

```sql
SELECT event_id, event_type, attempts, dead_lettered_at, last_error
FROM outbox_events
WHERE event_id = '00000000-0000-0000-0000-000000000001';
```

Replace the illustrative UUID with the inspected event ID. If the event was
quarantined by the old retry policy during a temporary outage, repair the cause
and make that exact unpublished row eligible again:

```sql
UPDATE outbox_events
SET dead_lettered_at = NULL, next_attempt_at = CURRENT_TIMESTAMP
WHERE event_id = '00000000-0000-0000-0000-000000000001'
  AND published_at IS NULL
  AND dead_lettered_at IS NOT NULL
  AND claim_id IS NULL
RETURNING event_id, attempts, next_attempt_at;
```

This preserves the event ID, original snapshot, attempt count and last error.
Verify `published_at` afterwards; an UPDATE alone is not evidence of publication.
Already-published later events are not reordered by this recovery. Downstream
consumers must still deduplicate by event ID.

Re-enabling a genuinely invalid immutable payload cannot repair it. Investigate
that defect and use the domain's correction mechanism to produce a new valid event;
do not rewrite historical payloads or claim delivery of the quarantined record.
For this challenge, the explicit quarantine remains visible through logs and
`outbox_dead_lettered_total`; transient failures never enter it automatically.
