package postgres

import (
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// The helpers below map Go zero values to SQL NULL and back: the domain uses
// "" / uuid.Nil / time.Time{} for "absent", the schema uses NULL.

// nullString maps "" to SQL NULL.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// deref maps SQL NULL to "".
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nullUUID maps uuid.Nil to SQL NULL.
func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

// derefUUID maps SQL NULL to uuid.Nil.
func derefUUID(id *uuid.UUID) uuid.UUID {
	if id == nil {
		return uuid.Nil
	}
	return *id
}

// nullTime maps the zero time to SQL NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// derefTime maps SQL NULL to the zero time.
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// resultColumns returns the result_balance_minor and result_currency column
// values of a transaction: both NULL while no balance was observed, both set
// once the operation concluded.
func resultColumns(t *wager.Transaction) (*int64, *string) {
	balance := t.ResultBalance()
	if balance.Validate() != nil {
		return nil, nil
	}
	minor, currency := balance.Minor(), string(balance.Currency())
	return &minor, &currency
}
