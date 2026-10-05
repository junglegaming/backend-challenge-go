package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

// Event types written to the outbox. They are the domain's names, because they
// are a contract with every consumer of the events.
const (
	eventWagerTransactionProcessed = string(domain.EventWagerTransactionProcessed)
	eventWalletBalanceChanged      = string(domain.EventWalletBalanceChanged)
)

// insertEvents records events inside the caller's transaction. They commit
// together with the state they describe, so an event exists if and only if the
// change it describes committed.
func insertEvents(ctx context.Context, tx pgx.Tx, events []domain.Event) error {
	for _, ev := range events {
		if err := insertEvent(ctx, tx, ev); err != nil {
			return err
		}
	}
	return nil
}

// insertEvent writes one event. The outbox row takes the event's own id, so
// every publication of this row, however many it takes, carries the same
// eventId. The payload is the whole envelope, as a snapshot.
func insertEvent(ctx context.Context, tx pgx.Tx, ev domain.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("postgres: encode %s event: %w", ev.Type, err)
	}

	const query = `INSERT INTO outbox (id, aggregate_id, event_type, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`
	if _, err := tx.Exec(ctx, query, ev.ID, ev.AggregateID, string(ev.Type), string(raw), ev.OccurredAt); err != nil {
		return fmt.Errorf("postgres: insert outbox %s: %w", ev.Type, err)
	}
	return nil
}
