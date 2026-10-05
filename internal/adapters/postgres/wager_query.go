package postgres

import (
	"context"
	"errors"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// WagerReader reads operations without locking them. The GET endpoints only
// need a snapshot, so they never hold a lock that writers would wait for.
type WagerReader struct {
	pool *Pool
}

// NewWagerReader builds a WagerReader backed by pool.
func NewWagerReader(pool *Pool) *WagerReader {
	return &WagerReader{pool: pool}
}

// FindByID returns the operation with this internal id, or ErrWagerNotFound.
func (r *WagerReader) FindByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	ctx, cancel := r.pool.withAcquireTimeout(ctx)
	defer cancel()

	query := `SELECT ` + wagerColumns + ` FROM wager_transactions WHERE id = $1`
	rec, err := scanWagerRecord(r.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrWagerNotFound
	}
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWagerTransaction(rec), nil
}

// Explicação

// - WagerReader existe para não colocar leitura dentro do WagerProcessor, que é o caminho de escrita com lock.
// - scanWagerRecord devolve pgx.ErrNoRows sem embrulhar, então o errors.Is aqui funciona.
