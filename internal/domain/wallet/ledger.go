package wallet

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

// Direction is the side of a ledger entry from the wallet point of view.
type Direction string

// Ledger directions.
const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// Valid reports whether d is a known direction.
func (d Direction) Valid() bool { return d == Debit || d == Credit }

// signed returns +amount for credits and -amount for debits.
func (d Direction) signed(amount money.Money) money.Money {
	if d == Debit {
		return amount.Neg()
	}
	return amount
}

// LedgerEntry is an immutable, append-only movement of a wallet balance.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// LedgerEntryParams carries every field of a ledger entry.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount. It is used
// both to create and to rehydrate entries, as it has no side effects.
func NewLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	if err := p.validateIdentity(); err != nil {
		return LedgerEntry{}, err
	}
	if err := p.validateAmounts(); err != nil {
		return LedgerEntry{}, err
	}
	return LedgerEntry{
		id: p.ID, walletID: p.WalletID, transactionID: p.TransactionID, direction: p.Direction,
		amount: p.Amount, balanceBefore: p.BalanceBefore, balanceAfter: p.BalanceAfter,
		createdAt: p.CreatedAt.UTC(),
	}, nil
}

func (p LedgerEntryParams) validateIdentity() error {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return fmt.Errorf("%w: missing identifiers", ErrInvalidLedgerEntry)
	}
	if !p.Direction.Valid() || p.CreatedAt.IsZero() {
		return fmt.Errorf("%w: invalid direction or timestamp", ErrInvalidLedgerEntry)
	}
	return nil
}

func (p LedgerEntryParams) validateSigns() error {
	if !p.Amount.IsPositive() || p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return fmt.Errorf("%w: amounts must be positive and balances non-negative", ErrInvalidLedgerEntry)
	}
	return nil
}

func (p LedgerEntryParams) validateAmounts() error {
	if err := p.validateSigns(); err != nil {
		return err
	}
	expected, err := p.BalanceBefore.Add(p.Direction.signed(p.Amount))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return fmt.Errorf("%w: balanceAfter %s != balanceBefore %s %s %s",
			ErrInvalidLedgerEntry, p.BalanceAfter, p.BalanceBefore, p.Direction, p.Amount)
	}
	return nil
}

// ID returns the entry identifier.
func (e LedgerEntry) ID() uuid.UUID { return e.id }

// WalletID returns the wallet identifier.
func (e LedgerEntry) WalletID() uuid.UUID { return e.walletID }

// TransactionID returns the originating transaction identifier.
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }

// Direction returns DEBIT or CREDIT.
func (e LedgerEntry) Direction() Direction { return e.direction }

// Amount returns the moved amount (always positive).
func (e LedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore returns the balance before the movement.
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the balance after the movement.
func (e LedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// CreatedAt returns the creation instant.
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }
