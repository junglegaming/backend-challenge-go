package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// OpenWallet stores a new wallet with its opening in one transaction. Either
// all of it commits or none of it does. A duplicate (player, currency) fails
// on the wallet row and rolls back the OPENING and the events with it.
func (r *WalletRepository) OpenWallet(ctx context.Context, open domain.WalletOpening) error {
	ctx, cancel := r.pool.withAcquireTimeout(ctx)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin opening tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := insertWallet(ctx, tx, open.Wallet); err != nil {
		return err
	}
	if open.Operation != nil {
		if err := insertWagerTransaction(ctx, tx, open.Operation); err != nil {
			return err
		}
		if err := r.InsertLedgerEntry(ctx, tx, open.Entry); err != nil {
			return err
		}
		if err := insertOpeningEvents(ctx, tx, open); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit opening tx: %w", err)
	}

	return nil
}

// insertWallet is the transactional twin of Create. The wallet must exist
// before its OPENING, because wager_transactions.wallet_id references it.
func insertWallet(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `
              INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7)
      `
	_, err := tx.Exec(ctx, query,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().AmountMinor(), w.Version(),
		w.CreatedAt(), w.UpdatedAt(),
	)
	if isUniqueViolation(err) {
		return ErrWalletAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("postgres: insert wallet: %w", err)
	}

	return nil
}

// insertOpeningEvents records what the opening did, as the same events an
// operation produces: WagerTransactionProcessed for the OPENING and
// WalletBalanceChanged for the wallet. The correlation id is the one of the
// request that opened the wallet.
func insertOpeningEvents(ctx context.Context, tx pgx.Tx, open domain.WalletOpening) error {
	events := domain.EventsFor(open.Operation, open.Entry, open.Wallet.Version(), logctx.CorrelationID(ctx))
	return insertEvents(ctx, tx, events)
}

// isUniqueViolation reports whether err is Postgres' unique_violation (23505).
// errors.As works on a nil error too, and returns false.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Explicação

// - OpenWallet abre uma transação só. A ordem importa: a carteira vem primeiro, porque wager_transactions.wallet_id é uma chave estrangeira para ela.
// - Quando a carteira duplicada falha com 23505, o defer Rollback desfaz tudo que já foi escrito nessa transação. Isso é o que o teste de duplicata verifica:
// nenhum segundo OPENING, nenhum lançamento e nenhum evento.
// - Os eventos são gravados na mesma transação do estado que descrevem. Esse é o padrão de outbox do §6.5 do README. O publisher que vai ler essas linhas ainda não existe,
// e isso fica documentado.
// - isUniqueViolation é uma função separada porque o mesmo mapeamento aparece em mais de um lugar. A versão do InsertLedgerEntry ainda faz o teste inline; pode migrar para
// essa função depois sem mudar o comportamento.
