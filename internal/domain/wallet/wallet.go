// Package wallet contains the Wallet aggregate root and its ledger entries.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

// InitialVersion is the version of a freshly opened wallet.
const InitialVersion int64 = 1

var (
	// ErrInvalidWallet reports invalid wallet data.
	ErrInvalidWallet = errors.New("wallet: invalid wallet")
	// ErrInvalidLedgerEntry reports an inconsistent ledger entry.
	ErrInvalidLedgerEntry = errors.New("wallet: invalid ledger entry")
	// ErrInvalidMovement reports a non-positive movement amount.
	ErrInvalidMovement = errors.New("wallet: movement amount must be positive")
	// ErrCurrencyMismatch reports a movement in a different currency.
	ErrCurrencyMismatch = errors.New("wallet: currency mismatch")
	// ErrInsufficientFunds reports a debit larger than the balance.
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
)

// Wallet is the financial aggregate root. Its balance only changes through
// Apply, which always produces the matching ledger entry.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// OpenParams describes the opening of a wallet.
type OpenParams struct {
	ID             uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	OpeningTxID    uuid.UUID
	OpeningEntryID uuid.UUID
	Now            time.Time
}

// Open creates a wallet at version 1. A positive initial balance produces the
// opening credit entry (the version stays 1); zero produces no entry.
func Open(p OpenParams) (*Wallet, *LedgerEntry, error) {
	if err := p.validate(); err != nil {
		return nil, nil, err
	}
	now := p.Now.UTC()
	w := &Wallet{id: p.ID, playerID: p.PlayerID, balance: p.InitialBalance,
		version: InitialVersion, createdAt: now, updatedAt: now}
	if p.InitialBalance.IsZero() {
		return w, nil, nil
	}
	zero, _ := money.Zero(p.InitialBalance.Currency()) // currency already validated by validBalance
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID: p.OpeningEntryID, WalletID: p.ID, TransactionID: p.OpeningTxID, Direction: Credit,
		Amount: p.InitialBalance, BalanceBefore: zero, BalanceAfter: p.InitialBalance, CreatedAt: now,
	})
	if err != nil {
		return nil, nil, err
	}
	return w, &entry, nil
}

func (p OpenParams) validate() error {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil || p.Now.IsZero() {
		return fmt.Errorf("%w: missing identity or timestamp", ErrInvalidWallet)
	}
	return validBalance(p.InitialBalance)
}

func validBalance(b money.Money) error {
	if b.Validate() != nil || b.IsNegative() {
		return fmt.Errorf("%w: balance must be a non-negative amount", ErrInvalidWallet)
	}
	return nil
}

// Snapshot is the persisted state used to rehydrate a wallet.
type Snapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate rebuilds a wallet from storage without replaying movements.
func Rehydrate(s Snapshot) (*Wallet, error) {
	if err := s.validIdentity(); err != nil {
		return nil, err
	}
	if err := s.validTimes(); err != nil {
		return nil, err
	}
	if err := validBalance(s.Balance); err != nil {
		return nil, err
	}
	return &Wallet{id: s.ID, playerID: s.PlayerID, balance: s.Balance, version: s.Version,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC()}, nil
}

func (s Snapshot) validIdentity() error {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil || s.Version < InitialVersion {
		return fmt.Errorf("%w: invalid snapshot identity or version", ErrInvalidWallet)
	}
	return nil
}

func (s Snapshot) validTimes() error {
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return fmt.Errorf("%w: invalid snapshot timestamps", ErrInvalidWallet)
	}
	return nil
}

// Movement describes a single debit or credit applied to the wallet.
type Movement struct {
	EntryID       uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	Now           time.Time
}

// Apply debits or credits the wallet, bumping its version and returning the
// ledger entry that must be persisted in the same SQL transaction.
func (w *Wallet) Apply(m Movement) (LedgerEntry, error) {
	if err := w.checkMovement(m); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Add(m.Direction.signed(m.Amount))
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, w.balance, m.Amount)
	}
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID: m.EntryID, WalletID: w.id, TransactionID: m.TransactionID, Direction: m.Direction,
		Amount: m.Amount, BalanceBefore: w.balance, BalanceAfter: after, CreatedAt: m.Now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = entry.CreatedAt()
	return entry, nil
}

func (w *Wallet) checkMovement(m Movement) error {
	if m.Amount.Validate() != nil || !m.Amount.IsPositive() {
		return ErrInvalidMovement
	}
	if m.Amount.Currency() != w.Currency() {
		return fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.Currency(), m.Amount.Currency())
	}
	return nil
}

// CanDebit reports whether amount can be debited without going negative. It
// is false when amount is uninitialized or in another currency.
func (w *Wallet) CanDebit(amount money.Money) bool {
	cmp, err := w.balance.Cmp(amount)
	return err == nil && cmp >= 0
}

// ID returns the wallet identifier.
func (w *Wallet) ID() uuid.UUID { return w.id }

// PlayerID returns the owner identifier.
func (w *Wallet) PlayerID() uuid.UUID { return w.playerID }

// Currency returns the wallet currency.
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }

// Balance returns the current balance.
func (w *Wallet) Balance() money.Money { return w.balance }

// Version returns the optimistic concurrency version.
func (w *Wallet) Version() int64 { return w.version }

// CreatedAt returns the creation instant.
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt returns the last balance change instant.
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }
