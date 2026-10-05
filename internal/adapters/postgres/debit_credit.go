package postgres

import (
	"context"
	"fmt"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

// DebitWallet locks walletID, debits amount from it (domain.Wallet.Debit
// decides whether that is allowed), and persists the new balance and the
// matching ledger entry — all inside one transaction. If the wallet does
// not have enough funds, the transaction is rolled back and nothing is
// written; domain.ErrInsufficientFunds is returned unchanged so the caller
// can distinguish it from a transport/infrastructure failure.
func DebitWallet(ctx context.Context, pool *Pool, walletID, transactionID uuid.UUID, amount domain.Money) (*domain.WalletLedgerEntry, error) {
	return applyMovement(ctx, pool, walletID, func(w *domain.Wallet) (*domain.WalletLedgerEntry, error) {
		return w.Debit(transactionID, amount)
	})
}

// CreditWallet is the mirror of DebitWallet for credits.
func CreditWallet(ctx context.Context, pool *Pool, walletID, transactionID uuid.UUID, amount domain.Money) (*domain.WalletLedgerEntry, error) {
	return applyMovement(ctx, pool, walletID, func(w *domain.Wallet) (*domain.WalletLedgerEntry, error) {
		return w.Credit(transactionID, amount)
	})
}

// applyMovement is the one place that owns the transaction: begin -> lock
// the wallet row -> let the domain decide -> save the wallet and the
// ledger entry -> commit. Any error at any step rolls everything back, so
// a rejected movement (insufficient funds, currency mismatch, overflow)
// never leaves a half-applied wallet or an orphaned ledger row behind.
//
// This is deliberately the only concurrency-sensitive code in the whole
// program: domain.Wallet itself has no idea Postgres exists, and the
// future use-case layer (once WagerTransaction exists) will call this
// function instead of re-implementing locking.
func applyMovement(ctx context.Context, pool *Pool, walletID uuid.UUID, move func(*domain.Wallet) (*domain.WalletLedgerEntry, error)) (*domain.WalletLedgerEntry, error) {
	ctx, cancel := pool.withAcquireTimeout(ctx)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin tx: %w", err)
	}
	// A no-op after a successful Commit (returns pgx.ErrTxClosed, ignored).
	defer func() { _ = tx.Rollback(ctx) }()

	repo := NewWalletRepository(pool)

	wallet, err := repo.LockForUpdate(ctx, tx, walletID)
	if err != nil {
		return nil, err
	}
	previousVersion := wallet.Version()

	entry, err := move(wallet)
	if err != nil {
		return nil, err // A business rejection (insufficient funds, etc.) is returned as-is;
		// the deferred Rollback above still runs, so nothing is written.
	}

	if err := repo.Save(ctx, tx, wallet, previousVersion); err != nil {
		return nil, err
	}
	if err := repo.InsertLedgerEntry(ctx, tx, entry); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: commit tx: %w", err)
	}
	return entry, nil
}

// Explicação

// Esse é o arquivo que prova a concorrência — equivalente direto ao LockingTransferExecutor.Execute do wallet-go.
// DebitWallet/CreditWallet são só casca fina; toda a lógica de verdade está em applyMovement, que recebe uma função
// (move) em vez de hardcodar "debitar" — assim o mesmo esqueleto de transação serve pra débito e crédito sem duplicar begin/lock/save/commit.

// Se perguntarem "por que não ficou dentro do WalletRepository?" — porque orquestrar uma transação (decidir quando começa e termina)
// é uma responsabilidade diferente de "ler e escrever uma linha". O repositório não sabe que existe um Debit; só sabe fazer SQL. Separar
// os dois é o que deixa o repositório reaproveitável também pra leituras simples (FindByID) sem transação nenhuma.
