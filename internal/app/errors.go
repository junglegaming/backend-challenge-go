// Package app holds the use cases shared by every entry point (HTTP, SQS and
// background workers) and the ports they need from the infrastructure.
package app

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrValidation reports a malformed request; it is never persisted and the
	// caller may correct it and retry with the same idempotency key.
	ErrValidation = errors.New("validation failed")
	// ErrWalletNotFound reports an unknown wallet.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrWalletExists reports a second wallet for the same player and currency.
	ErrWalletExists = errors.New("wallet already exists for player and currency")
	// ErrTransactionNotFound reports an unknown (or not visible) transaction.
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrIdempotencyConflict reports a reused key with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrDuplicateExternalID reports an external id reused with another key.
	ErrDuplicateExternalID = errors.New("external transaction already registered with another idempotency key")
	// ErrInboxConflict reports a redelivered message whose content changed.
	ErrInboxConflict = errors.New("message id reused with a different payload")
	// ErrForbidden reports an authenticated caller acting outside its scope.
	ErrForbidden = errors.New("forbidden")
	// ErrConflict reports a lost race (unique violation, stale version,
	// deadlock or serialization failure). It is transient and retried.
	ErrConflict = errors.New("concurrent modification")
	// ErrUnavailable reports a transient infrastructure failure.
	ErrUnavailable = errors.New("dependency temporarily unavailable")
	// ErrNotDue reports that a pending transaction was concluded or claimed
	// by another instance.
	ErrNotDue = errors.New("pending transaction is not due")
)

// errInvalidCursor reports a ledger cursor that was not produced by Ledger.
var errInvalidCursor = errors.New("invalid cursor")

// invalid wraps a domain or parsing error as ErrValidation.
func invalid(err error) error {
	return fmt.Errorf("%w: %w", ErrValidation, err)
}

// IsTransient reports whether retrying the same operation may succeed.
func IsTransient(err error) bool {
	return errors.Is(err, ErrConflict) || errors.Is(err, ErrUnavailable) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
