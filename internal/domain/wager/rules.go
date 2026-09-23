package wager

import (
	"slices"

	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Action is the outcome chosen by Decide.
type Action int

// Decision outcomes.
const (
	ActionProcess Action = iota + 1
	ActionReject
	ActionAwait
)

// Decision tells the application how to conclude an operation.
type Decision struct {
	// Action is what to do: process, reject or wait for the reference.
	Action Action
	// Code is the rejection reason; set only when Action is ActionReject.
	Code FailureCode
	// Moves reports whether processing changes the balance (false for LOSS).
	Moves bool
	// Direction is the wallet movement; meaningful only when Moves is true.
	Direction wallet.Direction
}

// DecisionInput gathers everything the rules need. Reference is nil when the
// referenced operation has not arrived; ReferenceReversed reports whether it
// already has a successful REFUND or ROLLBACK.
type DecisionInput struct {
	Wallet            *wallet.Wallet
	Transaction       *Transaction
	Reference         *Transaction
	ReferenceReversed bool
}

func reject(code FailureCode) Decision { return Decision{Action: ActionReject, Code: code} }

// Decide applies the business rules of the five external kinds. It is pure:
// it never mutates the wallet or the transaction.
//
// Preconditions: Wallet and Transaction are non-nil, the transaction is an
// external kind (never OPENING) in a non-terminal status, and Reference, when
// present, is the operation named by ReferenceExternalID.
func Decide(in DecisionInput) Decision {
	if code := checkOwnership(in.Wallet, in.Transaction); code != "" {
		return reject(code)
	}
	if d, done := decideReference(in); done {
		return d
	}
	return decideMovement(in.Wallet, in.Transaction, in.Reference)
}

// checkOwnership rejects a player/wallet mismatch. Callers load the wallet by
// the transaction's wallet id, so the wallet-id comparison is only a cheap
// invariant guard; the player comparison is the actual business rule.
func checkOwnership(w *wallet.Wallet, t *Transaction) FailureCode {
	if w.PlayerID() != t.PlayerID() || w.ID() != t.WalletID() {
		return CodePlayerWalletMismatch
	}
	if w.Currency() != t.Amount().Currency() {
		return CodeCurrencyMismatch
	}
	return ""
}

func decideReference(in DecisionInput) (Decision, bool) {
	t, ref := in.Transaction, in.Reference
	if t.External().ReferenceExternalID == "" {
		return Decision{}, false
	}
	if ref == nil || !ref.Status().Terminal() {
		return Decision{Action: ActionAwait}, true
	}
	if code := checkReference(t, ref, in.ReferenceReversed); code != "" {
		return reject(code), true
	}
	return Decision{}, false
}

func checkReference(t, ref *Transaction, reversed bool) FailureCode {
	if ref.Status() != StatusProcessed {
		return CodeReferenceNotProcessed
	}
	if !sameContext(t, ref) {
		return CodeReferenceMismatch
	}
	return checkTarget(t, ref, reversed)
}

func checkTarget(t, ref *Transaction, reversed bool) FailureCode {
	if !reversible(t.Kind(), ref.Kind()) {
		return CodeReferenceKindInvalid
	}
	if t.Kind().IsReversal() && !t.Amount().Equal(ref.Amount()) {
		return CodeReversalAmountMismatch
	}
	if t.Kind().IsReversal() && reversed {
		return CodeAlreadyReversed
	}
	return ""
}

// sameContext requires provider, player, wallet, currency and round to match.
func sameContext(t, ref *Transaction) bool {
	a, b := t.External(), ref.External()
	sameOwner := t.PlayerID() == ref.PlayerID() && t.WalletID() == ref.WalletID()
	return sameOwner && a.ProviderID == b.ProviderID && a.RoundID == b.RoundID &&
		t.Amount().Currency() == ref.Amount().Currency()
}

// referenceTargets lists which kinds each referencing kind may point to.
var referenceTargets = map[Kind][]Kind{
	KindWin:      {KindBet},
	KindRefund:   {KindBet},
	KindRollback: {KindBet, KindWin, KindRefund},
}

func reversible(kind, target Kind) bool {
	return slices.Contains(referenceTargets[kind], target)
}

// rollbackDirection maps the kind being rolled back to the movement that
// undoes it. It must cover every target listed for ROLLBACK in
// referenceTargets; anything else fails closed.
var rollbackDirection = map[Kind]wallet.Direction{
	KindBet: wallet.Credit, KindWin: wallet.Debit, KindRefund: wallet.Debit,
}

// directionOf returns the wallet movement of a kind. ROLLBACK moves opposite
// to the operation it undoes. ok is false when no direction is known, which
// includes LOSS (never moves money) and a ROLLBACK of an unmapped target.
func directionOf(kind Kind, ref *Transaction) (wallet.Direction, bool) {
	switch kind {
	case KindBet:
		return wallet.Debit, true
	case KindWin, KindRefund:
		return wallet.Credit, true
	case KindRollback:
		if ref == nil {
			return "", false
		}
		d, ok := rollbackDirection[ref.Kind()]
		return d, ok
	}
	return "", false
}

func decideMovement(w *wallet.Wallet, t *Transaction, ref *Transaction) Decision {
	if t.Kind() == KindLoss {
		return Decision{Action: ActionProcess}
	}
	dir, ok := directionOf(t.Kind(), ref)
	if !ok {
		return reject(CodeReferenceKindInvalid)
	}
	if code := checkLimits(w, t, dir); code != "" {
		return reject(code)
	}
	return Decision{Action: ActionProcess, Moves: true, Direction: dir}
}

// checkLimits verifies the balance can absorb the movement: debits need
// funds, credits must not overflow the balance.
func checkLimits(w *wallet.Wallet, t *Transaction, dir wallet.Direction) FailureCode {
	if dir == wallet.Debit {
		if !w.CanDebit(t.Amount()) {
			return insufficientFundsCode(t.Kind())
		}
		return ""
	}
	if _, err := w.Balance().Add(t.Amount()); err != nil {
		return CodeBalanceLimitExceeded
	}
	return ""
}

// insufficientFundsCode keeps reversal shortfalls distinguishable from bets.
func insufficientFundsCode(kind Kind) FailureCode {
	if kind.IsReversal() {
		return CodeReversalInsufficientFunds
	}
	return CodeInsufficientFunds
}
