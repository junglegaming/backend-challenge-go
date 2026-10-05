package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgUniqueViolation is the Postgres SQLSTATE for unique_violation.
const pgUniqueViolation = "23505"

// WalletRepository reads and writes the wallets and wallet_ledger_entries
// tables. Every method that mutates state takes an already-open pgx.Tx: this
// repository never decides transaction boundaries itself, because the whole
// point of LockForUpdate is to hold a row lock across several statements
// (read, domain decision, write) inside one caller-controlled transaction.
type WalletRepository struct {
	pool *Pool
}

// NewWalletRepository builds a WalletRepository backed by pool.
func NewWalletRepository(pool *Pool) *WalletRepository {
	return &WalletRepository{pool: pool}
}

// Create inserts a brand new wallet row. Used once, when a wallet is opened;
// no lock is needed because the row does not exist yet for anyone to race on.
func (r *WalletRepository) Create(ctx context.Context, w *domain.Wallet) error {
	ctx, cancel := r.pool.withAcquireTimeout(ctx)
	defer cancel()

	const query = `
              INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7)
    `
	_, err := r.pool.Exec(ctx, query,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().AmountMinor(), w.Version(),
		w.CreatedAt(), w.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf("postgres: create wallet: %w", err)
	}
	return nil
}

// FindByID reads a wallet without locking it — for read-only endpoints
// (balance, reconciliation), where serving a slightly stale snapshot is
// fine and holding a lock would only hurt writers for no reason.
func (r *WalletRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	ctx, cancel := r.pool.withAcquireTimeout(ctx)
	defer cancel()

	const query = `
              SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
              FROM wallets
              WHERE id = $1
      `
	return r.scanWallet(r.pool.QueryRow(ctx, query, id))
}

// LockForUpdate reads a wallet inside tx with SELECT ... FOR UPDATE: the row
// stays locked until tx commits or rolls back, so any other transaction
// trying to lock the same wallet blocks here until this one finishes. That
// is the entire concurrency mechanism — not a mutex, not application-level
// coordination, just the database serializing access to one row.
func (r *WalletRepository) LockForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error) {
	const query = `
              SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
              FROM wallets
              WHERE id = $1
              FOR UPDATE
      `
	return r.scanWallet(tx.QueryRow(ctx, query, id))
}

func (r *WalletRepository) scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balanceMinor, vers   int64
		createdAt, updatedAt time.Time
	)
	err := row.Scan(&id, &playerID, &currency, &balanceMinor, &vers, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("postgres: scan wallet: %w", err)
	}

	balance, err := domain.NewMoney(balanceMinor, currency)
	if err != nil {
		return nil, fmt.Errorf("postgres: rebuild balance: %w", err)
	}
	return domain.RehydrateWallet(id, playerID, currency, balance, vers, createdAt, updatedAt), nil
}

// Save persists w's current balance and version, guarded by
// WHERE version = previousVersion. Under the pessimistic locking strategy
// (LockForUpdate held for the whole transaction) this clause should always
// match — it is a cheap extra safety net, not the mechanism that prevents
// lost updates. If it ever matched zero rows, something read the wallet
// outside of a lock, which is a bug worth surfacing loudly.
func (r *WalletRepository) Save(ctx context.Context, tx pgx.Tx, w *domain.Wallet, previousVersion int64) error {
	const query = `
              UPDATE wallets
              SET balance_minor = $1, version = $2, updated_at = $3
              WHERE id = $4 AND version = $5
      `
	tag, err := tx.Exec(ctx, query, w.Balance().AmountMinor(), w.Version(), w.UpdatedAt(), w.ID(), previousVersion)
	if err != nil {
		return fmt.Errorf("postgres: save wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrWalletVersionConflict
	}
	return nil
}

// InsertLedgerEntry writes one immutable ledger line. The database's own
// UNIQUE(wallet_id, transaction_id) constraint — not this code — is what
// actually guarantees the same transaction never double-applies to the
// same wallet; this method only translates that violation into a typed
// error the caller can act on.
func (r *WalletRepository) InsertLedgerEntry(ctx context.Context, tx pgx.Tx, e *domain.WalletLedgerEntry) error {
	const query = `
              INSERT INTO wallet_ledger_entries
                      (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, created_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `
	_, err := tx.Exec(ctx, query,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()),
		e.Amount().AmountMinor(), e.BalanceBefore().AmountMinor(), e.BalanceAfter().AmountMinor(),
		e.CreatedAt(),
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ErrDuplicateLedgerEntry
		}
		return fmt.Errorf("postgres: insert ledger entry: %w", err)
	}
	return nil
}

// Explicação
// LockForUpdate recebe um pgx.Tx de fora, não abre a própria transação. Isso é o ponto mais importante do arquivo: se o repositório abrisse
// e fechasse sua própria transação dentro de LockForUpdate, o lock seria liberado assim que a função retornasse — antes do domínio decidir
// e antes de salvar. O lock só vale a pena se ficar de pé durante todo o ciclo (ler → decidir → escrever), e isso só é possível se quem chama
// controla o início e o fim da transação. É por isso que a orquestração fica num arquivo separado (debit_credit.go).
// Save com WHERE version = $antigo: mesmo já tendo o lock, deixo essa checagem porque é de graça (zero custo extra, já estamos dentro da mesma transação)
// e é exatamente o tipo de "defesa em profundidade" que mostra que você entende a diferença entre "o mecanismo que garante" e "a rede de segurança que
// detectaria se o mecanismo falhasse".
// FindByID sem lock, de propósito: uma leitura de saldo pra mostrar numa tela não devia bloquear quem está tentando debitar. Só travo a linha quando realmente vou mudar o saldo.
