//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
)

func getDBPool(t *testing.T) *pgxpool.Pool {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://user:password@localhost:5432/wallet?sslmode=disable"
	}
	
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	return pool
}

func TestConcurrentTransactions(t *testing.T) {
	pool := getDBPool(t)
	defer pool.Close()

	repo := db.NewWalletRepository(&db.PostgresDB{Pool: pool})
	tm := &db.PostgresDB{Pool: pool}
	uc := usecases.NewProcessTransactionUseCase(tm, repo)

	ctx := context.Background()

	playerIDStr := uuid.New().String()
	walletIDStr := uuid.New().String()
	
	_, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance, created_at, updated_at) 
		VALUES ($1, $2, 'BRL', 10000, NOW(), NOW())
	`, walletIDStr, playerIDStr)
	require.NoError(t, err)

	providerID := uuid.New().String()
	
	var wg sync.WaitGroup
	errorsCh := make(chan error, 50)
	
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			txID := uuid.New().String()
			money, _ := domain.ParseMoney("1.00", "BRL")
			req := domain.ExternalRequest{
				ExternalTransactionID: txID,
				WalletID:              uuid.MustParse(walletIDStr),
				ProviderID:            providerID,
				PlayerID:              uuid.MustParse(playerIDStr),
				GameID:                "game-1",
				RoundID:               "round-1",
				Kind:                  domain.KindBet,
				Money:                 money,
			}
			_, err := uc.Execute(ctx, req, "", "")
			if err != nil {
				errorsCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errorsCh)

	for err := range errorsCh {
		assert.NoError(t, err)
	}

	var finalBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE id = $1", walletIDStr).Scan(&finalBalance)
	require.NoError(t, err)
	
	assert.Equal(t, int64(5000), finalBalance)
}

func TestIdempotencyPersistent(t *testing.T) {
	pool := getDBPool(t)
	defer pool.Close()

	repo := db.NewWalletRepository(&db.PostgresDB{Pool: pool})
	tm := &db.PostgresDB{Pool: pool}
	uc := usecases.NewProcessTransactionUseCase(tm, repo)

	ctx := context.Background()

	walletIDStr := uuid.New().String()
	playerIDStr := uuid.New().String()
	
	_, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance, created_at, updated_at) 
		VALUES ($1, $2, 'BRL', 10000, NOW(), NOW())
	`, walletIDStr, playerIDStr)
	require.NoError(t, err)

	providerID := uuid.New().String()
	txID := uuid.New().String()
	roundID := uuid.New().String()
	
	money, _ := domain.ParseMoney("10.00", "BRL")
	req := domain.ExternalRequest{
		ExternalTransactionID: txID,
		WalletID:              uuid.MustParse(walletIDStr),
		ProviderID:            providerID,
		PlayerID:              uuid.MustParse(playerIDStr),
		GameID:                "game-1",
		RoundID:               roundID,
		Kind:                  domain.KindBet,
		Money:                 money,
	}

	res1, err := uc.Execute(ctx, req, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, res1.Status())

	res2, err := uc.Execute(ctx, req, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, res2.Status())
	
	var finalBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE id = $1", walletIDStr).Scan(&finalBalance)
	require.NoError(t, err)
	assert.Equal(t, int64(9000), finalBalance)
}

func TestReversalArrivalBeforeBet(t *testing.T) {
	pool := getDBPool(t)
	defer pool.Close()

	repo := db.NewWalletRepository(&db.PostgresDB{Pool: pool})
	tm := &db.PostgresDB{Pool: pool}
	uc := usecases.NewProcessTransactionUseCase(tm, repo)

	ctx := context.Background()

	walletIDStr := uuid.New().String()
	playerIDStr := uuid.New().String()
	
	_, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance, created_at, updated_at) 
		VALUES ($1, $2, 'BRL', 10000, NOW(), NOW())
	`, walletIDStr, playerIDStr)
	require.NoError(t, err)

	providerID := uuid.New().String()
	betTxID := uuid.New().String()
	rollbackTxID := uuid.New().String()
	roundID := uuid.New().String()

	money, _ := domain.ParseMoney("10.00", "BRL")
	rollbackReq := domain.ExternalRequest{
		ExternalTransactionID:          rollbackTxID,
		WalletID:                       uuid.MustParse(walletIDStr),
		ProviderID:                     providerID,
		PlayerID:                       uuid.MustParse(playerIDStr),
		GameID:                         "game-1",
		RoundID:                        roundID,
		Kind:                           domain.KindRollback,
		Money:                          money,
		ReferenceExternalTransactionID: betTxID,
	}

	resRollback, err := uc.Execute(ctx, rollbackReq, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPendingReference, resRollback.Status())

	betReq := domain.ExternalRequest{
		ExternalTransactionID: betTxID,
		WalletID:              uuid.MustParse(walletIDStr),
		ProviderID:            providerID,
		PlayerID:              uuid.MustParse(playerIDStr),
		GameID:                "game-1",
		RoundID:               roundID,
		Kind:                  domain.KindBet,
		Money:                 money,
	}

	resBet, err := uc.Execute(ctx, betReq, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, resBet.Status())

	pendingUc := usecases.NewPendingReferenceUseCase(tm, repo)
	_, err = pendingUc.ProcessBatch(ctx, 10, time.Hour)
	require.NoError(t, err)

	var finalBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE id = $1", walletIDStr).Scan(&finalBalance)
	require.NoError(t, err)
	assert.Equal(t, int64(10000), finalBalance)
}
