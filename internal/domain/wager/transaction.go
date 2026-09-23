package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

// maxFieldLength bounds every free-text external identifier.
const maxFieldLength = 128

// External holds the provider metadata of an external operation.
type External struct {
	// ProviderID identifies the game provider that submitted the operation.
	ProviderID string
	// ExternalID is the provider's transaction identifier, unique per provider.
	ExternalID string
	// IdempotencyKey is the caller supplied key used to deduplicate retries.
	IdempotencyKey string
	// PayloadHash is Fingerprint.Hash() of the business fields, used to detect
	// a reused idempotency key with a different payload.
	PayloadHash string
	// RoundID groups the operations of one game round.
	RoundID string
	// GameID identifies the game the round belongs to.
	GameID string
	// ReferenceExternalID names the provider transaction this one refers to;
	// empty when the kind carries no reference.
	ReferenceExternalID string
}

func (e External) validate(kind Kind) error {
	fields := []struct{ name, value string }{
		{"providerId", e.ProviderID}, {"externalTransactionId", e.ExternalID}, {"idempotencyKey", e.IdempotencyKey},
		{"payloadHash", e.PayloadHash}, {"roundId", e.RoundID}, {"gameId", e.GameID},
	}
	for _, f := range fields {
		if f.value == "" || len(f.value) > maxFieldLength {
			return fmt.Errorf("%w: %s must be non-empty and at most %d bytes", ErrInvalidTransaction, f.name, maxFieldLength)
		}
	}
	if e.ReferenceExternalID != "" && e.ReferenceExternalID == e.ExternalID {
		return fmt.Errorf("%w: an operation cannot reference itself", ErrInvalidTransaction)
	}
	return validateReference(kind, e.ReferenceExternalID)
}

func validateReference(kind Kind, ref string) error {
	if kind.RequiresReference() && ref == "" {
		return fmt.Errorf("%w: %s requires referenceExternalTransactionId", ErrInvalidTransaction, kind)
	}
	if !kind.AcceptsReference() && ref != "" {
		return fmt.Errorf("%w: %s does not accept a reference", ErrInvalidTransaction, kind)
	}
	if len(ref) > maxFieldLength {
		return fmt.Errorf("%w: reference too long", ErrInvalidTransaction)
	}
	return nil
}

// Transaction is a wager operation. Its state only changes through the
// explicit transition methods, which refuse to leave a terminal state.
type Transaction struct {
	id            uuid.UUID
	origin        Origin
	kind          Kind
	status        Status
	walletID      uuid.UUID
	playerID      uuid.UUID
	amount        money.Money
	external      External
	referenceTxID uuid.UUID
	failureCode   FailureCode
	resultBalance money.Money
	attempts      int
	nextAttemptAt time.Time
	correlationID string
	createdAt     time.Time
	updatedAt     time.Time
}

// ExternalParams describes a provider operation.
type ExternalParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	PlayerID      uuid.UUID
	Kind          Kind
	Amount        money.Money
	External      External
	CorrelationID string
	Now           time.Time
}

// NewExternal creates a PENDING provider operation, enforcing the zero-value
// policy: LOSS requires exactly 0.00, every other kind requires a positive amount.
func NewExternal(p ExternalParams) (*Transaction, error) {
	if !p.Kind.External() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKind, p.Kind)
	}
	if err := validateCore(p.ID, p.WalletID, p.PlayerID, p.Now); err != nil {
		return nil, err
	}
	if err := validateAmount(p.Kind, p.Amount); err != nil {
		return nil, err
	}
	if err := p.External.validate(p.Kind); err != nil {
		return nil, err
	}
	now := p.Now.UTC()
	return &Transaction{id: p.ID, origin: OriginExternal, kind: p.Kind, status: StatusPending,
		walletID: p.WalletID, playerID: p.PlayerID, amount: p.Amount, external: p.External,
		correlationID: p.CorrelationID, createdAt: now, updatedAt: now}, nil
}

// OpeningParams describes the internal opening credit of a wallet.
type OpeningParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	PlayerID      uuid.UUID
	Amount        money.Money
	CorrelationID string
	Now           time.Time
}

// NewOpening creates the PENDING internal OPENING transaction. Provider,
// external ids, idempotency data, round, game and reference do not apply.
func NewOpening(p OpeningParams) (*Transaction, error) {
	if err := validateCore(p.ID, p.WalletID, p.PlayerID, p.Now); err != nil {
		return nil, err
	}
	if err := validateAmount(KindOpening, p.Amount); err != nil {
		return nil, err
	}
	now := p.Now.UTC()
	return &Transaction{id: p.ID, origin: OriginInternal, kind: KindOpening, status: StatusPending,
		walletID: p.WalletID, playerID: p.PlayerID, amount: p.Amount,
		correlationID: p.CorrelationID, createdAt: now, updatedAt: now}, nil
}

func validateCore(id, walletID, playerID uuid.UUID, now time.Time) error {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil || now.IsZero() {
		return fmt.Errorf("%w: missing identifiers or timestamp", ErrInvalidTransaction)
	}
	return nil
}

func validateAmount(kind Kind, amount money.Money) error {
	if err := amount.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTransaction, err)
	}
	if kind.RequiresZeroAmount() != amount.IsZero() || amount.IsNegative() {
		return fmt.Errorf("%w: %s amount %s violates the zero-value policy", ErrInvalidTransaction, kind, amount.Amount())
	}
	return nil
}

// Snapshot is the persisted state used to rehydrate a transaction.
type Snapshot struct {
	ID            uuid.UUID
	Origin        Origin
	Kind          Kind
	Status        Status
	WalletID      uuid.UUID
	PlayerID      uuid.UUID
	Amount        money.Money
	External      External
	ReferenceTxID uuid.UUID
	FailureCode   FailureCode
	ResultBalance money.Money
	Attempts      int
	NextAttemptAt time.Time
	CorrelationID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Rehydrate rebuilds a transaction without re-running transitions. Stored
// data is validated too, so corrupt rows fail loudly instead of reaching
// the rules (e.g. a reversal without a reference).
func Rehydrate(s Snapshot) (*Transaction, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &Transaction{id: s.ID, origin: s.Origin, kind: s.Kind, status: s.Status,
		walletID: s.WalletID, playerID: s.PlayerID, amount: s.Amount, external: s.External,
		referenceTxID: s.ReferenceTxID, failureCode: s.FailureCode, resultBalance: s.ResultBalance,
		attempts: s.Attempts, nextAttemptAt: s.NextAttemptAt.UTC(), correlationID: s.CorrelationID,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC()}, nil
}

func (t *Transaction) ensureOpen(target Status, now time.Time) error {
	if t == nil {
		return fmt.Errorf("%w: uninitialized transaction", ErrInvalidTransaction)
	}
	if t.status != StatusPending && t.status != StatusPendingReference {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, target)
	}
	if err := t.snapshot().validate(); err != nil {
		return err
	}
	return validateTimestamp(now, t.updatedAt)
}

func (t *Transaction) snapshot() Snapshot {
	return Snapshot{ID: t.id, Origin: t.origin, Kind: t.kind, Status: t.status,
		WalletID: t.walletID, PlayerID: t.playerID, Amount: t.amount, External: t.external,
		ReferenceTxID: t.referenceTxID, FailureCode: t.failureCode, ResultBalance: t.resultBalance,
		Attempts: t.attempts, NextAttemptAt: t.nextAttemptAt, CorrelationID: t.correlationID,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt}
}

// Process moves the transaction to PROCESSED, recording the balance returned
// to the provider and the resolved reference. The reference must be uuid.Nil
// when no external reference was supplied and must be set when one was supplied.
func (t *Transaction) Process(balance money.Money, referenceTxID uuid.UUID, now time.Time) error {
	if err := t.ensureOpen(StatusProcessed, now); err != nil {
		return err
	}
	if err := requireProcessedBalance(balance, t.amount); err != nil {
		return err
	}
	if err := t.checkResolvedReference(referenceTxID); err != nil {
		return err
	}
	t.status, t.resultBalance, t.referenceTxID = StatusProcessed, balance, referenceTxID
	t.touch(now)
	return nil
}

func (t *Transaction) checkResolvedReference(referenceTxID uuid.UUID) error {
	if referenceTxID != uuid.Nil && !t.kind.AcceptsReference() {
		return fmt.Errorf("%w: %s cannot resolve a reference", ErrInvalidTransaction, t.kind)
	}
	if (referenceTxID != uuid.Nil) != (t.external.ReferenceExternalID != "") {
		return fmt.Errorf("%w: resolved and external references must agree", ErrInvalidTransaction)
	}
	return nil
}

// Reject moves the transaction to REJECTED with a business failure code and
// the balance observed at decision time.
func (t *Transaction) Reject(code FailureCode, observed money.Money, now time.Time) error {
	if err := t.ensureOpen(StatusRejected, now); err != nil {
		return err
	}
	if err := requireCode(code, "rejection"); err != nil {
		return err
	}
	if err := requireBalance(observed, "rejected"); err != nil {
		return err
	}
	t.status, t.failureCode, t.resultBalance = StatusRejected, code, observed
	t.touch(now)
	return nil
}

// Fail moves the transaction to FAILED after a permanent infrastructure error.
func (t *Transaction) Fail(code FailureCode, now time.Time) error {
	if err := t.ensureOpen(StatusFailed, now); err != nil {
		return err
	}
	if err := requireCode(code, "failure"); err != nil {
		return err
	}
	t.status, t.failureCode = StatusFailed, code
	t.touch(now)
	return nil
}

func requireCode(code FailureCode, what string) error {
	if code == "" {
		return fmt.Errorf("%w: %s requires a failure code", ErrInvalidTransaction, what)
	}
	return nil
}

func requireProcessedBalance(balance, amount money.Money) error {
	if err := requireBalance(balance, "processed"); err != nil {
		return err
	}
	if balance.Currency() != amount.Currency() {
		return fmt.Errorf("%w: processed balance currency differs from amount", ErrInvalidTransaction)
	}
	return nil
}

func requireBalance(balance money.Money, what string) error {
	if balance.Validate() != nil || balance.IsNegative() {
		return fmt.Errorf("%w: %s transaction needs a result balance", ErrInvalidTransaction, what)
	}
	return nil
}

// AwaitReference moves the transaction to PENDING_REFERENCE, counting the
// attempt and scheduling the next resolution try.
func (t *Transaction) AwaitReference(next, now time.Time) error {
	if err := t.ensureOpen(StatusPendingReference, now); err != nil {
		return err
	}
	if !t.kind.AcceptsReference() || t.external.ReferenceExternalID == "" {
		return fmt.Errorf("%w: %s has no reference to wait for", ErrInvalidTransition, t.kind)
	}
	if err := validateTimestamp(next, now); err != nil {
		return err
	}
	t.status = StatusPendingReference
	t.attempts++
	t.nextAttemptAt = next.UTC()
	t.touch(now)
	return nil
}

func (t *Transaction) touch(now time.Time) { t.updatedAt = now.UTC() }

// ID returns the internal identifier.
func (t *Transaction) ID() uuid.UUID { return t.id }

// Origin returns EXTERNAL or INTERNAL.
func (t *Transaction) Origin() Origin { return t.origin }

// Kind returns the operation kind.
func (t *Transaction) Kind() Kind { return t.kind }

// Status returns the current state.
func (t *Transaction) Status() Status { return t.status }

// WalletID returns the wallet identifier.
func (t *Transaction) WalletID() uuid.UUID { return t.walletID }

// PlayerID returns the player identifier.
func (t *Transaction) PlayerID() uuid.UUID { return t.playerID }

// Amount returns the operation amount.
func (t *Transaction) Amount() money.Money { return t.amount }

// External returns the provider metadata (empty for internal operations).
func (t *Transaction) External() External { return t.external }

// ReferenceTxID returns the resolved internal reference or uuid.Nil.
func (t *Transaction) ReferenceTxID() uuid.UUID { return t.referenceTxID }

// FailureCode returns the rejection or failure code.
func (t *Transaction) FailureCode() FailureCode { return t.failureCode }

// ResultBalance returns the balance observed when the operation concluded.
// It is the zero Money while the operation has not concluded and also after
// Fail, which records no balance.
func (t *Transaction) ResultBalance() money.Money { return t.resultBalance }

// Attempts returns how many times the reference resolution was deferred.
func (t *Transaction) Attempts() int { return t.attempts }

// NextAttemptAt returns when the reference resolution should run again.
func (t *Transaction) NextAttemptAt() time.Time { return t.nextAttemptAt }

// CorrelationID returns the correlation identifier of the originating request.
func (t *Transaction) CorrelationID() string { return t.correlationID }

// CreatedAt returns the creation instant.
func (t *Transaction) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt returns the last transition instant.
func (t *Transaction) UpdatedAt() time.Time { return t.updatedAt }
