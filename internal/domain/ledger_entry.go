package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Direction is which way a WalletLedgerEntry moves money.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// WalletLedgerEntry is one immutable line of a wallet's ledger. It is never
// constructed directly by callers outside this package — only Wallet.Debit
// and Wallet.Credit create one, which is what lets newWalletLedgerEntry
// guarantee the balanceAfter = balanceBefore ± amount invariant by
// construction, instead of relying on every caller to get the arithmetic
// right.
type WalletLedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

// newWalletLedgerEntry recomputes balanceAfter from balanceBefore, amount
// and direction, and rejects the entry if the caller's balanceAfter does
// not match — the entry does not trust the caller's arithmetic, it checks it.
func newWalletLedgerEntry(walletID, transactionID uuid.UUID, direction Direction, amount, balanceBefore, balanceAfter Money) (*WalletLedgerEntry, error) {
	var (
		expected Money
		err      error
	)
	switch direction {
	case DirectionDebit:
		expected, err = balanceBefore.Sub(amount)
	case DirectionCredit:
		expected, err = balanceBefore.Add(amount)
	default:
		return nil, fmt.Errorf("domain: unknown ledger direction %q", direction)
	}
	if err != nil {
		return nil, err
	}
	if !expected.Equal(balanceAfter) {
		return nil, fmt.Errorf("domain: ledger invariant violated: balanceAfter does not match balanceBefore %s amount", direction)
	}

	return &WalletLedgerEntry{
		id:            uuid.New(),
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     time.Now().UTC(),
	}, nil
}

func (e *WalletLedgerEntry) ID() uuid.UUID            { return e.id }
func (e *WalletLedgerEntry) WalletID() uuid.UUID      { return e.walletID }
func (e *WalletLedgerEntry) TransactionID() uuid.UUID { return e.transactionID }
func (e *WalletLedgerEntry) Direction() Direction     { return e.direction }
func (e *WalletLedgerEntry) Amount() Money            { return e.amount }
func (e *WalletLedgerEntry) BalanceBefore() Money     { return e.balanceBefore }
func (e *WalletLedgerEntry) BalanceAfter() Money      { return e.balanceAfter }
func (e *WalletLedgerEntry) CreatedAt() time.Time     { return e.createdAt }

// O README pede, ao pé da letra: "sua construção deve validar balanceAfter = balanceBefore ± money" (§6.4).
// A forma mais fácil de garantir isso de verdade (não só documentar que deveria ser assim) é recalcular o
// balanceAfter dentro do construtor e comparar com o que foi passado — se alguém, um dia, escrever um bug em outro
// lugar do código que monte um WalletLedgerEntry com números que não se encaixam, o próprio construtor recusa,
//  em vez de confiar. É por isso que newWalletLedgerEntry é minúsculo (privado ao pacote): a única forma de
//  criar uma entry é passando pelo Wallet.Debit/Credit, que são os únicos lugares que já têm o saldo anterior em mãos.

// Se perguntarem "por que Direction é uma string tipada em vez de um int/enum numérico?" — porque isso
// é dado que vai pro banco e pra JSON. "DEBIT"/"CREDIT" legível direto na linha do banco/log vale mais que economizar 3 bytes com um inteiro.
