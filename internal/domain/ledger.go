package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

// NewLedgerEntry valida balanceAfter = balanceBefore ± amount; serve para criação e reidratação.
func NewLedgerEntry(id, walletID, transactionID uuid.UUID, dir Direction, amount, before, after Money, createdAt time.Time) (LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil || !dir.Valid() {
		return LedgerEntry{}, fmt.Errorf("%w: lançamento incompleto", ErrInvalidArgument)
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: lançamento deve ser positivo", ErrInvalidAmount)
	}
	signed := amount
	if dir == Debit {
		var err error
		if signed, err = amount.Neg(); err != nil {
			return LedgerEntry{}, err
		}
	}
	expected, err := before.Add(signed)
	if err != nil {
		return LedgerEntry{}, err
	}
	if !expected.Equal(after) || after.IsNegative() || before.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: saldo posterior inconsistente", ErrInvalidArgument)
	}
	return LedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: dir, amount: amount, balanceBefore: before, balanceAfter: after, createdAt: createdAt.UTC()}, nil
}

func LedgerEntryFromMovement(id, walletID, transactionID uuid.UUID, m Movement, at time.Time) (LedgerEntry, error) {
	return NewLedgerEntry(id, walletID, transactionID, m.Direction, m.Amount, m.BalanceBefore, m.BalanceAfter, at)
}

func (e LedgerEntry) ID() uuid.UUID            { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID      { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }
func (e LedgerEntry) Direction() Direction     { return e.direction }
func (e LedgerEntry) Amount() Money            { return e.amount }
func (e LedgerEntry) BalanceBefore() Money     { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() Money      { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time     { return e.createdAt }
