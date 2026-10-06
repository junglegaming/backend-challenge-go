package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"go.uber.org/fx"

	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
	infrahttp "github.com/Lzdevmendes/backend-challenge-go/internal/infra/http"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/sqs"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/workers"
)

func main() {
	fx.New(
		fx.Provide(
			// Config
			func() string {
				dsn := os.Getenv("DATABASE_URL")
				if dsn == "" {
					dsn = "postgres://user:password@localhost:5432/wallet?sslmode=disable"
				}
				return dsn
			},

			// Database Connection
			func(lc fx.Lifecycle, dsn string) (*db.PostgresDB, error) {
				ctx := context.Background()
				postgresDB, err := db.NewPostgresDB(ctx, dsn)
				if err != nil {
					return nil, err
				}

				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						postgresDB.Close()
						return nil
					},
				})

				return postgresDB, nil
			},

			// Repositories
			func(postgresDB *db.PostgresDB) *db.WalletRepository {
				return db.NewWalletRepository(postgresDB)
			},

			// Provide TransactionManager and WalletRepository interfaces
			func(postgresDB *db.PostgresDB) usecases.TransactionManager {
				return postgresDB
			},
			func(walletRepo *db.WalletRepository) usecases.WalletRepository {
				return walletRepo
			},

			// UseCases
			usecases.NewCreateWalletUseCase,
			usecases.NewProcessTransactionUseCase,
			usecases.NewQueryUseCase,
			usecases.NewPendingReferenceUseCase,

			// SQS Config
			func() sqs.SQSConfig {
				queueURL := os.Getenv("SQS_QUEUE_URL")
				if queueURL == "" {
					queueURL = "http://localhost:4566/000000000000/wager-transactions.fifo"
				}
				endpoint := os.Getenv("SQS_ENDPOINT")
				if endpoint == "" {
					endpoint = "http://localhost:4566"
				}
				return sqs.SQSConfig{
					QueueURL:   queueURL,
					Region:     "us-east-1",
					Endpoint:   endpoint,
					MaxRetries: 3,
				}
			},

			// Provide SQS Client
			sqs.NewClient,

			// SQS Consumer
			sqs.NewConsumer,

			// Background Workers
			workers.NewOutboxWorker,
			workers.NewPendingReferenceWorker,

			// Auth
			func(lc fx.Lifecycle) (*infrahttp.AuthMiddleware, error) {
				ctx := context.Background()
				issuerURL := os.Getenv("KEYCLOAK_ISSUER_URL") // e.g. "http://localhost:8180/realms/wallet-realm"
				return infrahttp.NewAuthMiddleware(ctx, issuerURL)
			},

			// HTTP Handler
			infrahttp.NewHandler,

			// HTTP Server
			infrahttp.NewServer,
		),
		fx.Invoke(
			// Start HTTP Server, Consumer, and Workers
			func(srv *http.Server, consumer *sqs.Consumer, outboxWorker *workers.OutboxWorker, pendingWorker *workers.PendingReferenceWorker) {
				fmt.Printf("Starting HTTP Server on %s\n", srv.Addr)
			},
		),
	).Run()
}
