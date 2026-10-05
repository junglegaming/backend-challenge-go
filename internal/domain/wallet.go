package domain

import (
	"time"

	"github.com/google/uuid"
)

// Wallet is the financial aggregate root: identity, player, currency,
// balance and version are all owned here, and the only way to change the
// balance is through Debit/Credit, which also produce the matching
// WalletLedgerEntry. There is no stored-balance-vs-ledger drift possible,
// because both change together, in memory, before anything is persisted.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// NewWallet creates a brand new wallet at version 1. initialBalance must be
// in the same currency and must not be negative — zero is accepted (the
// challenge explicitly allows an opening with zero balance to skip the
// ledger/event side effects entirely; that decision lives one layer up,
// in the use case, not here).
func NewWallet(playerID uuid.UUID, currency string, initialBalance Money) (*Wallet, error) {
	if initialBalance.Currency() != currency {
		return nil, ErrCurrencyMismatch
	}
	if initialBalance.IsNegative() {
		return nil, ErrInvalidAmount
	}

	now := time.Now().UTC()
	return &Wallet{
		id:        uuid.New(),
		playerID:  playerID,
		currency:  currency,
		balance:   initialBalance,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// RehydrateWallet rebuilds a Wallet from already-persisted state. Unlike
// NewWallet, it does not validate or recompute anything — the row came from
// the database, which is where the invariants were already enforced when it
// was written. Rehydration never replays Debit/Credit and never produces a
// ledger entry; that would double-count history that already happened.
func RehydrateWallet(id, playerID uuid.UUID, currency string, balance Money, version int64, createdAt, updatedAt time.Time) *Wallet {
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Currency() string     { return w.currency }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Debit subtracts amount from the balance. It never lets the balance go
// negative: that check happens here, in the domain, so it holds regardless
// of which concurrency strategy the storage adapter uses to serialize
// access to this wallet's row.
func (w *Wallet) Debit(transactionID uuid.UUID, amount Money) (*WalletLedgerEntry, error) {
	entry, newBalance, err := w.applyMovement(transactionID, DirectionDebit, amount)
	if err != nil {
		return nil, err
	}
	w.commit(newBalance)
	return entry, nil
}

// Credit adds amount to the balance.
func (w *Wallet) Credit(transactionID uuid.UUID, amount Money) (*WalletLedgerEntry, error) {
	entry, newBalance, err := w.applyMovement(transactionID, DirectionCredit, amount)
	if err != nil {
		return nil, err
	}
	w.commit(newBalance)
	return entry, nil
}

// applyMovement computes the entry and the resulting balance without
// mutating the wallet, so that neither Debit nor Credit ever leaves the
// wallet half-changed if a later check (insufficient funds, overflow) fails.
// A debit or credit of zero or less is not a movement. LOSS (which
// is exactly a zero-amount operation in this challenge) must never
// reach Debit/Credit at all — the caller simply does not call them
// for it, and produces no ledger entry, per the challenge's rule.
func (w *Wallet) applyMovement(transactionID uuid.UUID, direction Direction, amount Money) (*WalletLedgerEntry, Money, error) {
	if amount.Currency() != w.currency {
		return nil, Money{}, ErrCurrencyMismatch
	}
	if !amount.IsPositive() {
		return nil, Money{}, ErrInvalidAmount
	}

	var newBalance Money
	var err error
	switch direction {
	case DirectionDebit:
		newBalance, err = w.balance.Sub(amount)
	case DirectionCredit:
		newBalance, err = w.balance.Add(amount)
	}
	if err != nil {
		return nil, Money{}, err
	}
	if direction == DirectionDebit && newBalance.IsNegative() {
		return nil, Money{}, ErrInsufficientFunds
	}

	entry, err := newWalletLedgerEntry(w.id, transactionID, direction, amount, w.balance, newBalance)
	if err != nil {
		return nil, Money{}, err
	}
	return entry, newBalance, nil
}

func (w *Wallet) commit(newBalance Money) {
	w.balance = newBalance
	w.version++
	w.updatedAt = time.Now().UTC()
}

// NewWallet vs RehydrateWallet: o README exige isso explicitamente ("separe criação e reidratação. A reidratação não deve reaplicar movimentações").
// A diferença prática: NewWallet decide coisas (versão começa em 1, valida moeda, valida saldo inicial); RehydrateWallet só monta uma struct com dados
// que o Postgres já validou quando foram escritos. Se você usasse NewWallet para carregar do banco, toda leitura reiniciaria a versão em 1 — bug sutil e feio.

// applyMovement como função auxiliar só pra evitar mutação parcial: se Debit calculasse w.balance = ... antes de checar saldo insuficiente, e a checagem viesse depois,
// um caminho de erro mal escrito deixaria a carteira num estado inconsistente mesmo sem comitar no banco (bug local, mas ainda um bug). Separar "calcula o que aconteceria"
//  de "aplica de verdade" (commit) é uma forma barata de garantir que erro nunca muta estado — o tipo de coisa que um revisor sênior comentaria se não existisse.

// Onde está o lock de concorrência? Não está aqui. Wallet.Debit roda inteiramente em memória — não sabe de Postgres, SELECT FOR UPDATE, nem transação. Isso é proposital
// (mesma regra do wallet-go: domínio não importa infraestrutura). A camada de adapter (próximo passo) vai: abrir transação → SELECT ... FOR UPDATE na linha da carteira →
// RehydrateWallet com o que leu → chamar Debit/Credit → persistir o novo saldo e a entry, com WHERE version = $antigo como checagem extra. Se perguntarem "e se dois processos
// tentarem debitar a mesma carteira ao mesmo tempo?" — a resposta é: o segundo processo bloqueia no SELECT FOR UPDATE até o primeiro comitar, então ele sempre lê o saldo já
// atualizado antes de decidir.
