package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

type inboxRepo struct{ db dbtx }

// Register inserts the message or, on redelivery, returns the stored entry.
// A concurrent delivery of the same message blocks on the unique key until
// the first one commits or rolls back.
func (r inboxRepo) Register(ctx context.Context, consumer, messageID, hash string, now time.Time) (app.InboxEntry, bool, error) {
	var inserted bool
	err := r.db.QueryRow(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING true`, consumer, messageID, hash, now).Scan(&inserted)
	if err == nil {
		return app.InboxEntry{PayloadHash: hash}, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return app.InboxEntry{}, false, mapError(err)
	}
	var entry app.InboxEntry
	err = r.db.QueryRow(ctx, `SELECT payload_hash, processed_at IS NOT NULL FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&entry.PayloadHash, &entry.Completed)
	return entry, false, mapError(err)
}

// Complete marks the registered message as processed, linking the resulting
// transaction (uuid.Nil when the message produced none). Completing a
// message that was never registered is a consumer bug and is reported as an
// error rather than silently ignored.
func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error {
	tag, err := r.db.Exec(ctx, `UPDATE inbox_messages SET processed_at = $4, transaction_id = $3
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, nullUUID(transactionID), now)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("inbox: message %s/%s not registered", consumer, messageID)
	}
	return nil
}
