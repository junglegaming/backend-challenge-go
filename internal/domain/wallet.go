package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func (d Direction) Valid() bool { return d == Debit || d == Credit }

func (d Direction) Opposite() Direction {
	if d == Debit {
		return Credit
	}
	return Debit
}

// Movement descreve uma mudança de saldo já aplicada ao agregado.
type Movement struct {
	Direction     Direction
	Amount        Money
	BalanceBefore Money
	BalanceAfter  Money
	WalletVersion int64
}

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  Currency
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(id, playerID uuid.UUID, initial Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids da carteira são obrigatórios", ErrInvalidArgument)
	}
	if !initial.IsInitialized() {
		return nil, ErrUninitialized
	}
	if initial.IsNegative() {
		return nil, fmt.Errorf("%w: saldo inicial negativo", ErrInvalidAmount)
	}
	now = now.UTC()
	return &Wallet{id: id, playerID: playerID, currency: initial.Currency(), balance: initial, version: 1, createdAt: now, updatedAt: now}, nil
}

// RehydrateWallet reconstrói o estado persistido sem aplicar movimentações.
func RehydrateWallet(id, playerID uuid.UUID, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil || !balance.IsInitialized() || balance.IsNegative() || version < 1 {
		return nil, fmt.Errorf("%w: estado de carteira inválido", ErrInvalidArgument)
	}
	return &Wallet{id: id, playerID: playerID, currency: balance.Currency(), balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Currency() Currency   { return w.currency }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

func (w *Wallet) Debit(amount Money, now time.Time) (Movement, error) {
	return w.apply(Debit, amount, now)
}

func (w *Wallet) Credit(amount Money, now time.Time) (Movement, error) {
	return w.apply(Credit, amount, now)
}

func (w *Wallet) Apply(dir Direction, amount Money, now time.Time) (Movement, error) {
	return w.apply(dir, amount, now)
}

func (w *Wallet) apply(dir Direction, amount Money, now time.Time) (Movement, error) {
	if !dir.Valid() {
		return Movement{}, fmt.Errorf("%w: direção %q", ErrInvalidArgument, dir)
	}
	if !amount.IsInitialized() {
		return Movement{}, ErrUninitialized
	}
	if amount.Currency() != w.currency {
		return Movement{}, fmt.Errorf("%w: carteira %s, movimento %s", ErrCurrencyMismatch, w.currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return Movement{}, fmt.Errorf("%w: movimento deve ser positivo", ErrInvalidAmount)
	}
	var after Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(amount)
		if err == nil && after.IsNegative() {
			return Movement{}, ErrInsufficientFunds
		}
	} else {
		after, err = w.balance.Add(amount)
	}
	if err != nil {
		return Movement{}, err
	}
	before := w.balance
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return Movement{Direction: dir, Amount: amount, BalanceBefore: before, BalanceAfter: after, WalletVersion: w.version}, nil
}
