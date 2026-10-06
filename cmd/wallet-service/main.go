package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"go.uber.org/fx"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
	infrahttp "github.com/Lzdevmendes/backend-challenge-go/internal/infra/http"
	infrasqs "github.com/Lzdevmendes/backend-challenge-go/internal/infra/sqs"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/workers"
)

var ConfigModule = fx.Provide(
	func() string {
		dsn := os.Getenv("DATABASE_URL")
		if dsn == "" {
			dsn = "postgres://user:password@localhost:5432/wallet?sslmode=disable"
		}
		return dsn
	},
	func() infrasqs.SQSConfig {
		queueURL := os.Getenv("SQS_QUEUE_URL")
		if queueURL == "" {
			queueURL = "http://localhost:4566/000000000000/wager-transactions.fifo"
		}
		endpoint := os.Getenv("SQS_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:4566"
		}
		return infrasqs.SQSConfig{
			QueueURL:   queueURL,
			Region:     "us-east-1",
			Endpoint:   endpoint,
			MaxRetries: 3,
		}
	},
	func() *slog.Logger {
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		slog.SetDefault(logger)
		return logger
	},
)

var DBModule = fx.Provide(
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
	func(postgresDB *db.PostgresDB) *db.WalletRepository {
		return db.NewWalletRepository(postgresDB)
	},
	func(postgresDB *db.PostgresDB) usecases.TransactionManager {
		return postgresDB
	},
	func(walletRepo *db.WalletRepository) usecases.WalletRepository {
		return walletRepo
	},
)

var SQSModule = fx.Provide(
	infrasqs.NewClient,
	infrasqs.NewConsumer,
)

var WorkersModule = fx.Provide(
	workers.NewOutboxWorker,
	workers.NewPendingReferenceWorker,
)

var UseCasesModule = fx.Provide(
	usecases.NewCreateWalletUseCase,
	usecases.NewProcessTransactionUseCase,
	usecases.NewQueryUseCase,
	usecases.NewPendingReferenceUseCase,
)

var HTTPModule = fx.Provide(
	func(lc fx.Lifecycle) (*infrahttp.AuthMiddleware, error) {
		ctx := context.Background()
		issuerURL := os.Getenv("KEYCLOAK_ISSUER_URL")
		return infrahttp.NewAuthMiddleware(ctx, issuerURL)
	},
	func(db *db.PostgresDB, sqsClient *sqs.Client) *infrahttp.HealthHandler {
		return infrahttp.NewHealthHandler(db, sqsClient)
	},
	infrahttp.NewHandler,
	infrahttp.NewServer,
)

func main() {
	fx.New(
		ConfigModule,
		DBModule,
		SQSModule,
		WorkersModule,
		UseCasesModule,
		HTTPModule,
		fx.Invoke(
			func(srv *http.Server, consumer *infrasqs.Consumer, outboxWorker *workers.OutboxWorker, pendingWorker *workers.PendingReferenceWorker, logger *slog.Logger) {
				logger.Info(fmt.Sprintf("Application started, listening on %s", srv.Addr))
			},
		),
	).Run()
}
