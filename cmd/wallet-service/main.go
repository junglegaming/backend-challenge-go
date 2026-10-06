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
			func() *db.WalletRepository {
				return db.NewWalletRepository()
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

			// HTTP Handler
			infrahttp.NewHandler,

			// HTTP Server
			infrahttp.NewServer,
		),
		fx.Invoke(
			// Start HTTP Server
			func(srv *http.Server) {
				fmt.Printf("Starting HTTP Server on %s\n", srv.Addr)
			},
		),
	).Run()
}
