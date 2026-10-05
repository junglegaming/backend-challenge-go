package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// WagerKind is the type of an operation sent by a game provider.
type WagerKind string

const (
	// WagerKindOpening is reserved for the internal opening of a wallet. It
	// is never accepted from HTTP or SQS.
	WagerKindOpening  WagerKind = "OPENING"
	WagerKindBet      WagerKind = "BET"
	WagerKindWin      WagerKind = "WIN"
	WagerKindLoss     WagerKind = "LOSS"
	WagerKindRefund   WagerKind = "REFUND"
	WagerKindRollback WagerKind = "ROLLBACK"
)

// WagerState is where an operation is in its lifecycle.
type WagerState string

const (
	WagerStatePending          WagerState = "PENDING"
	WagerStatePendingReference WagerState = "PENDING_REFERENCE"
	WagerStateProcessed        WagerState = "PROCESSED"
	WagerStateRejected         WagerState = "REJECTED"
	WagerStateFailed           WagerState = "FAILED"
)

// FailureCode is the stable, documented reason an operation was not applied.
// Clients branch on these strings, so they must never change once published.
type FailureCode string

// FailureReversalInsufficientFunds: a reversal would debit more than the
// wallet holds. Kept distinct from FailureInsufficientFunds on purpose:
// the README requires the two to be told apart in audit.
// FailureInsufficientFunds: a BET or WIN-side debit exceeded the balance.
const (
	FailureInsufficientFunds         FailureCode = "insufficient_funds"
	FailureReversalInsufficientFunds FailureCode = "reversal_insufficient_funds"
	FailureReferenceNotFound         FailureCode = "reference_not_found"
	FailureReferenceNotProcessed     FailureCode = "reference_not_processed"
	FailureReferenceMismatch         FailureCode = "reference_mismatch"
	FailureReferenceAmountMismatch   FailureCode = "reference_amount_mismatch"
	FailureDuplicateReversal         FailureCode = "duplicate_reversal"
	FailureReferenceExpired          FailureCode = "reference_expired"
)

// WagerCommand is the input needed to build a WagerTransaction. It carries
// only what the caller provides; identity, state and timestamps are assigned
// by NewWagerTransaction.
type WagerCommand struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	RequestHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           WagerKind
	Amount                         Money
	ReferenceExternalTransactionID string
}

// WagerTransaction is one operation sent by a provider, and its lifecycle.
// Its fields are private: the only way to change its state is through the
// transition methods, which enforce the state machine.
type WagerTransaction struct {
	id                             uuid.UUID
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	requestHash                    string
	walletID                       uuid.UUID
	playerID                       uuid.UUID
	roundID                        string
	gameID                         string
	kind                           WagerKind
	amount                         Money
	referenceExternalTransactionID string
	referenceTransactionID         uuid.UUID
	state                          WagerState
	failureCode                    FailureCode
	balanceAfter                   *Money
	createdAt                      time.Time
	updatedAt                      time.Time
}

// NewWagerTransaction validates cmd against the challenge's rules for its
// kind and returns a transaction in PENDING. Every rejection here is an
// input problem (ErrInvalidWagerTransaction / ErrInvalidAmount), not a
// business outcome, so nothing is persisted for it.
func NewWagerTransaction(cmd WagerCommand) (*WagerTransaction, error) {
	if err := validateWagerCommand(cmd); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	return &WagerTransaction{
		id:                             uuid.New(),
		providerID:                     cmd.ProviderID,
		externalTransactionID:          cmd.ExternalTransactionID,
		idempotencyKey:                 cmd.IdempotencyKey,
		requestHash:                    cmd.RequestHash,
		walletID:                       cmd.WalletID,
		playerID:                       cmd.PlayerID,
		roundID:                        cmd.RoundID,
		gameID:                         cmd.GameID,
		kind:                           cmd.Kind,
		amount:                         cmd.Amount,
		referenceExternalTransactionID: cmd.ReferenceExternalTransactionID,
		state:                          WagerStatePending,
		createdAt:                      now,
		updatedAt:                      now,
	}, nil
}

func validateWagerCommand(cmd WagerCommand) error {
	required := map[string]string{
		"provider id":             cmd.ProviderID,
		"external transaction id": cmd.ExternalTransactionID,
		"idempotency key":         cmd.IdempotencyKey,
		"request hash":            cmd.RequestHash,
		"round id":                cmd.RoundID,
		"game id":                 cmd.GameID,
	}
	for name, value := range required {
		if value == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidWagerTransaction, name)
		}
	}
	if cmd.WalletID == uuid.Nil || cmd.PlayerID == uuid.Nil {
		return fmt.Errorf("%w: wallet id and player id are required", ErrInvalidWagerTransaction)
	}

	switch cmd.Kind {
	case WagerKindBet, WagerKindWin, WagerKindLoss, WagerKindRefund, WagerKindRollback:
	case WagerKindOpening:
		return fmt.Errorf("%w: OPENING is internal and cannot come from an external provider", ErrInvalidWagerTransaction)
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidWagerTransaction, cmd.Kind)
	}

	// LOSS is a zero-amount operation by definition; every other kind moves money.
	if cmd.Kind == WagerKindLoss {
		if !cmd.Amount.IsZero() {
			return fmt.Errorf("%w: LOSS must have amount 0", ErrInvalidWagerTransaction)
		}
	} else if !cmd.Amount.IsPositive() {
		return fmt.Errorf("%w: %s amount must be greater than zero", ErrInvalidAmount, cmd.Kind)
	}

	hasReference := cmd.ReferenceExternalTransactionID != ""
	switch cmd.Kind {
	case WagerKindRefund, WagerKindRollback:
		if !hasReference {
			return fmt.Errorf("%w: %s requires a reference external transaction id", ErrInvalidWagerTransaction, cmd.Kind)
		}
	case WagerKindBet, WagerKindLoss:
		if hasReference {
			return fmt.Errorf("%w: %s does not take a reference", ErrInvalidWagerTransaction, cmd.Kind)
		}
	}
	return nil
}

func (w *WagerTransaction) ID() uuid.UUID                 { return w.id }
func (w *WagerTransaction) ProviderID() string            { return w.providerID }
func (w *WagerTransaction) ExternalTransactionID() string { return w.externalTransactionID }
func (w *WagerTransaction) IdempotencyKey() string        { return w.idempotencyKey }
func (w *WagerTransaction) RequestHash() string           { return w.requestHash }
func (w *WagerTransaction) WalletID() uuid.UUID           { return w.walletID }
func (w *WagerTransaction) PlayerID() uuid.UUID           { return w.playerID }
func (w *WagerTransaction) RoundID() string               { return w.roundID }
func (w *WagerTransaction) GameID() string                { return w.gameID }
func (w *WagerTransaction) Kind() WagerKind               { return w.kind }
func (w *WagerTransaction) Amount() Money                 { return w.amount }
func (w *WagerTransaction) ReferenceExternalTransactionID() string {
	return w.referenceExternalTransactionID
}
func (w *WagerTransaction) ReferenceTransactionID() uuid.UUID { return w.referenceTransactionID }
func (w *WagerTransaction) State() WagerState                 { return w.state }
func (w *WagerTransaction) FailureCode() FailureCode          { return w.failureCode }
func (w *WagerTransaction) CreatedAt() time.Time              { return w.createdAt }
func (w *WagerTransaction) UpdatedAt() time.Time              { return w.updatedAt }

// BalanceAfter is the wallet balance observed when the operation was
// processed. It is nil until the operation is PROCESSED. Replays return this
// value, not the wallet's current balance.
func (w *WagerTransaction) BalanceAfter() *Money { return w.balanceAfter }

// IsTerminal reports whether the operation has reached a final state, which
// no transition may leave.
func (w *WagerTransaction) IsTerminal() bool {
	return w.state == WagerStateProcessed || w.state == WagerStateRejected || w.state == WagerStateFailed
}

// MarkProcessed records a successful application. Only a PENDING operation
// can be processed, and the observed balance is stored with it.
func (w *WagerTransaction) MarkProcessed(balanceAfter Money) error {
	if w.state != WagerStatePending {
		return fmt.Errorf("%w: cannot process from %s", ErrInvalidWagerTransition, w.state)
	}
	w.state = WagerStateProcessed
	w.failureCode = ""
	w.balanceAfter = &balanceAfter
	w.touch()
	return nil
}

// ResolveReference records the internal id of the referenced operation and
// returns a parked operation to PENDING so it can be processed.
func (w *WagerTransaction) ResolveReference(referenceID uuid.UUID) error {
	if w.state != WagerStatePendingReference {
		return fmt.Errorf("%w: cannot resolve a reference from %s", ErrInvalidWagerTransition, w.state)
	}
	if referenceID == uuid.Nil {
		return fmt.Errorf("%w: reference id is required", ErrInvalidWagerTransition)
	}
	w.state = WagerStatePending
	w.failureCode = ""
	w.referenceTransactionID = referenceID
	w.touch()
	return nil
}

// MarkRejected records a definitive business rejection. It needs a code so
// the rejection is always explainable to the provider and auditable.
func (w *WagerTransaction) MarkRejected(code FailureCode) error {
	return w.finish(WagerStateRejected, code)
}

func (w *WagerTransaction) finish(to WagerState, code FailureCode) error {
	if w.state != WagerStatePending && w.state != WagerStatePendingReference {
		return fmt.Errorf("%w: cannot move %s to %s", ErrInvalidWagerTransition, w.state, to)
	}
	if code == "" {
		return fmt.Errorf("%w: a failure code is required to move to %s", ErrInvalidWagerTransition, to)
	}
	w.state = to
	w.failureCode = code
	w.touch()
	return nil
}

func (w *WagerTransaction) touch() { w.updatedAt = time.Now().UTC() }

// ValidateReversal checks a REFUND or ROLLBACK against the operation it
// references, per the challenge's rules (§7). It returns the failure code a
// rejection must carry, or "" when the reversal may proceed.
//
// referenced is nil when the reference does not exist. alreadyReversed is
// true when a REFUND or ROLLBACK has already been successfully applied to the
// same referenced operation: the caller answers that from storage, because
// the domain alone cannot see other transactions.
func ValidateReversal(reversal, referenced *WagerTransaction, alreadyreversed bool) FailureCode {
	if referenced == nil {
		return FailureReferenceNotFound
	}
	if referenced.state != WagerStateProcessed {
		return FailureReferenceNotProcessed
	}
	if referenced.providerID != reversal.providerID || referenced.playerID != reversal.playerID || referenced.walletID != reversal.walletID || referenced.roundID != reversal.roundID || referenced.amount.Currency() != reversal.amount.Currency() {
		return FailureReferenceMismatch
	}
	if !kindCanBeReversedBy(reversal.kind, referenced.kind) {
		return FailureReferenceMismatch
	}
	if !reversal.amount.Equal(referenced.amount) {
		return FailureReferenceAmountMismatch
	}
	if alreadyreversed {
		return FailureDuplicateReversal
	}
	return ""
}

// kindCanBeReversedBy encodes which operation kinds each reversal may
// undo: a REFUND only returns a BET; a ROLLBACK can undo a BET, a WIN or a
// REFUND. Anything else is a mismatch.
func kindCanBeReversedBy(reversal, referenced WagerKind) bool {
	switch reversal {
	case WagerKindRefund:
		return referenced == WagerKindBet
	case WagerKindRollback:
		return referenced == WagerKindBet || referenced == WagerKindWin || referenced == WagerKindRefund
	default:
		return false
	}
}

// MarkPendingReference parks a PENDING operation because a reference it
// depends on has not arrived yet. The code says why it is waiting.
func (w *WagerTransaction) MarkPendingReference(code FailureCode) error {
	if w.state != WagerStatePending {
		return fmt.Errorf("%w: cannot wait for a reference from %s", ErrInvalidWagerTransition, w.state)
	}
	if code == "" {
		return fmt.Errorf("%w: a reason code is required", ErrInvalidWagerTransition)
	}
	w.state = WagerStatePendingReference
	w.failureCode = code
	w.touch()
	return nil
}

// MarkFailed records a permanent infrastructure failure, kept for audit.
func (w *WagerTransaction) MarkFailed(code FailureCode) error {
	return w.finish(WagerStateFailed, code)
}
