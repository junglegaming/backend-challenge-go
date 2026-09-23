package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
)

type outboxRepo struct{ db dbtx }

// Append stores the event snapshots; they become visible to publishers only
// when the surrounding transaction commits.
func (r outboxRepo) Append(ctx context.Context, records ...event.Record) error {
	for _, rec := range records {
		_, err := r.db.Exec(ctx, `INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			rec.EventID, rec.AggregateType, rec.AggregateID, rec.PartitionKey, rec.EventType, string(rec.Payload), rec.OccurredAt)
		if err != nil {
			return mapError(err)
		}
	}
	return nil
}

// OutboxStore implements app.OutboxStore.
type OutboxStore struct {
	pool *pgxpool.Pool
}

// NewOutboxStore builds the relay store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Claim leases the oldest immediately publishable partition head. A later
// event of a wallet remains blocked by its earlier unpublished event. The
// acquisition-specific claimID fences a relay from an event that was reclaimed
// after its lease expired, even when both acquisitions use the same owner.
func (s *OutboxStore) Claim(ctx context.Context, owner string, claimID uuid.UUID, now time.Time, lease time.Duration) (app.OutboxMessage, bool, error) {
	row := s.pool.QueryRow(ctx, `WITH heads AS (
			SELECT DISTINCT ON (partition_key) event_id
			FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL
			ORDER BY partition_key, seq), candidate AS (
			SELECT o.event_id FROM outbox_events o JOIN heads h ON h.event_id = o.event_id
			WHERE o.published_at IS NULL AND o.dead_lettered_at IS NULL
				AND o.next_attempt_at <= $2 AND (o.locked_until IS NULL OR o.locked_until < $2)
			ORDER BY o.seq LIMIT 1
			FOR UPDATE OF o SKIP LOCKED)
		UPDATE outbox_events
		SET locked_by = $1, locked_until = $3, claim_id = $4
		WHERE event_id = (SELECT event_id FROM candidate)
		RETURNING seq, event_id, event_type, aggregate_type, aggregate_id, partition_key,
			payload::text, occurred_at, attempts, claim_id`, owner, now, now.Add(lease), claimID)
	msg, err := scanOutboxMessage(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.OutboxMessage{}, false, nil
	}
	if err != nil {
		return app.OutboxMessage{}, false, mapError(err)
	}
	return msg, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOutboxMessage(row rowScanner) (app.OutboxMessage, error) {
	var m app.OutboxMessage
	var payload string
	err := row.Scan(&m.Seq, &m.EventID, &m.EventType, &m.AggregateType, &m.AggregateID, &m.PartitionKey, &payload, &m.OccurredAt, &m.Attempts, &m.ClaimID)
	m.Payload = []byte(payload)
	return m, err
}

// StartAttempt atomically renews a live claim from the beginning of the real
// broker attempt and increments its publication count. It reports false when
// the lease expired or another acquisition fenced the caller out.
func (s *OutboxStore) StartAttempt(
	ctx context.Context, eventID, claimID uuid.UUID, now time.Time, lease time.Duration,
) (int, bool, error) {
	var attempts int
	err := s.pool.QueryRow(ctx, `UPDATE outbox_events
		SET attempts = attempts + 1, locked_until = $4
		WHERE event_id = $1 AND claim_id = $2 AND locked_until > $3
			AND published_at IS NULL AND dead_lettered_at IS NULL
		RETURNING attempts`, eventID, claimID, now, now.Add(lease)).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, mapError(err)
	}
	return attempts, true, nil
}

// MarkPublished implements app.OutboxStore. It reports false when the lease
// is no longer held by claimID (another relay took over after it expired).
func (s *OutboxStore) MarkPublished(ctx context.Context, eventID, claimID uuid.UUID, now time.Time) (bool, error) {
	return s.release(ctx, `published_at = $3, last_error = NULL`, eventID, claimID, now)
}

// MarkFailed implements app.OutboxStore.
func (s *OutboxStore) MarkFailed(ctx context.Context, eventID, claimID uuid.UUID, next time.Time, cause string) (bool, error) {
	return s.release(ctx, `next_attempt_at = $3, last_error = $4`, eventID, claimID, next, cause)
}

// MarkDead implements app.OutboxStore.
func (s *OutboxStore) MarkDead(ctx context.Context, eventID, claimID uuid.UUID, now time.Time, cause string) (bool, error) {
	return s.release(ctx, `dead_lettered_at = $3, last_error = $4`, eventID, claimID, now, cause)
}

// release drops claimID's lease on the unpublished event while applying
// set (a SET clause whose placeholders start at $3, bound to args). The
// token guard prevents a stale acquisition from mutating a reclaimed record.
func (s *OutboxStore) release(ctx context.Context, set string, eventID, claimID uuid.UUID, args ...any) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL, claim_id = NULL, `+set+`
		WHERE event_id = $1 AND claim_id = $2 AND published_at IS NULL`, append([]any{eventID, claimID}, args...)...)
	if err != nil {
		return false, mapError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// OldestPending implements app.OutboxStore.
func (s *OutboxStore) OldestPending(ctx context.Context) (time.Time, bool, error) {
	var oldest *time.Time
	err := s.pool.QueryRow(ctx, `SELECT MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL`).Scan(&oldest)
	if err != nil || oldest == nil {
		return time.Time{}, false, mapError(err)
	}
	return *oldest, true, nil
}
