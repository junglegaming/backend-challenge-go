package usecases_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
)

func TestConcurrencyWalletBetting(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://user:password@localhost:5432/wallet?sslmode=disable"
	}

	ctx := context.Background()
	postgresDB, err := db.NewPostgresDB(ctx, dsn)
	if err != nil {
		t.Skipf("Skipping integration test: could not connect to database at %s: %v", dsn, err)
	}
	defer postgresDB.Close()

	repo := db.NewWalletRepository(postgresDB)
	createWalletUC := usecases.NewCreateWalletUseCase(postgresDB, repo)
	processTxUC := usecases.NewProcessTransactionUseCase(postgresDB, repo)

	// Create a new player and wallet with 0.00
	playerID := uuid.New()
	wallet, err := createWalletUC.Execute(ctx, usecases.CreateWalletCommand{
		PlayerID: playerID,
		Currency: "BRL",
	})
	require.NoError(t, err)
	require.NotNil(t, wallet)

	// Fund the wallet with 100.00
	fundMoney, _ := domain.ParseMoney("100.00", "BRL")
	fundReq := domain.ExternalRequest{
		ProviderID:            "provider-test",
		ExternalTransactionID: uuid.NewString(),
		PlayerID:              playerID,
		WalletID:              wallet.ID(),
		Kind:                  domain.KindWin,
		Money:                 fundMoney,
		IdempotencyKey:        uuid.NewString(),
	}
	_, err = processTxUC.Execute(ctx, fundReq, "", "")
	require.NoError(t, err)

	// Refresh wallet
	wallet, err = repo.FindWalletByID(ctx, wallet.ID())
	require.NoError(t, err)
	require.Equal(t, int64(10000), wallet.Balance().Units()) // 100.00

	concurrencyLevel := 10
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)

	results := make([]*domain.WagerTransaction, concurrencyLevel)
	errorsList := make([]error, concurrencyLevel)

	// We create 10 simultaneous bets of 80.00
	// Only 1 should succeed, 9 should be rejected for insufficient funds.
	for i := 0; i < concurrencyLevel; i++ {
		go func(idx int) {
			defer wg.Done()

			money, _ := domain.ParseMoney("80.00", "BRL")
			req := domain.ExternalRequest{
				ProviderID:            "provider-test",
				ExternalTransactionID: uuid.NewString(), // unique
				PlayerID:              playerID,
				WalletID:              wallet.ID(),
				Kind:                  domain.KindBet,
				Money:                 money,
				IdempotencyKey:        uuid.NewString(),
			}

			// Add a tiny sleep to increase chance of race condition
			// (if locking wasn't working)
			time.Sleep(10 * time.Millisecond)

			txResult, err := processTxUC.Execute(ctx, req, "", "")
			if err != nil {
				errorsList[idx] = err
			} else {
				results[idx] = txResult
			}
		}(i)
	}

	wg.Wait()

	processedCount := 0
	rejectedCount := 0

	for i := 0; i < concurrencyLevel; i++ {
		require.NoError(t, errorsList[i])
		res := results[i]
		require.NotNil(t, res)

		if res.Status() == domain.StatusProcessed {
			processedCount++
		} else if res.Status() == domain.StatusRejected && res.FailureCode() == domain.FailureInsufficientFunds {
			rejectedCount++
		} else {
			t.Errorf("Unexpected status/failure code: %s / %s", res.Status(), res.FailureCode())
		}
	}

	assert.Equal(t, 1, processedCount, "Exactly 1 transaction should be processed")
	assert.Equal(t, concurrencyLevel-1, rejectedCount, "The rest should be rejected due to insufficient funds")

	// Verify final wallet balance
	walletAfter, err := repo.FindWalletByID(ctx, wallet.ID())
	require.NoError(t, err)
	assert.Equal(t, int64(2000), walletAfter.Balance().Units(), "Final balance must be 20.00")
}
