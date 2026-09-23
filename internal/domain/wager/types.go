// Package wager models wager transactions, their state machine and the pure
// business rules that decide how an operation affects a wallet.
package wager

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidTransaction reports invalid transaction data.
	ErrInvalidTransaction = errors.New("wager: invalid transaction")
	// ErrInvalidKind reports an unknown or non-external kind.
	ErrInvalidKind = errors.New("wager: invalid kind")
	// ErrInvalidTransition reports a forbidden state transition.
	ErrInvalidTransition = errors.New("wager: invalid state transition")
)

// Kind is the type of a wager transaction.
type Kind string

// Transaction kinds. OPENING is internal only.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

var externalKinds = map[Kind]struct{}{
	KindBet: {}, KindWin: {}, KindLoss: {}, KindRefund: {}, KindRollback: {},
}

// ParseExternalKind parses a kind accepted from providers (HTTP or SQS).
// OPENING is reserved for internal wallet opening and is rejected.
func ParseExternalKind(s string) (Kind, error) {
	k := Kind(s)
	if !k.External() {
		return "", fmt.Errorf("%w: %q", ErrInvalidKind, s)
	}
	return k, nil
}

// External reports whether k may be submitted by a provider.
func (k Kind) External() bool {
	_, ok := externalKinds[k]
	return ok
}

// Valid reports whether k is any known kind, including OPENING.
func (k Kind) Valid() bool { return k.External() || k == KindOpening }

// RequiresReference reports whether the kind must reference another operation.
func (k Kind) RequiresReference() bool { return k == KindRefund || k == KindRollback }

// AcceptsReference reports whether the kind may carry a reference.
func (k Kind) AcceptsReference() bool { return k.RequiresReference() || k == KindWin }

// IsReversal reports whether the kind reverts a previous operation. Today it
// coincides with RequiresReference, but the two express different rules: a
// reversal must match the target amount and may happen only once, while a
// required reference is a validation constraint. Keeping them apart lets a
// future kind require a reference without being a reversal (as WIN accepts
// one without being).
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// RequiresZeroAmount reports whether the kind must carry exactly 0.00.
func (k Kind) RequiresZeroAmount() bool { return k == KindLoss }

// Status is the lifecycle state of a transaction.
type Status string

// Transaction states.
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

// Terminal reports whether no further transitions are allowed.
func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distinguishes provider operations from internal ones.
type Origin string

// Transaction origins.
const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

// FailureCode is a stable, documented rejection or failure reason.
type FailureCode string

// Failure codes of persisted rejections (definitive results) and failures.
const (
	CodeInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	CodePlayerWalletMismatch      FailureCode = "PLAYER_WALLET_MISMATCH"
	CodeBalanceLimitExceeded      FailureCode = "BALANCE_LIMIT_EXCEEDED"
	CodeReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	CodeReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	CodeReferenceKindInvalid      FailureCode = "REFERENCE_KIND_INVALID"
	CodeReversalAmountMismatch    FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	CodeAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	CodeInternalFailure           FailureCode = "INTERNAL_FAILURE"
)
