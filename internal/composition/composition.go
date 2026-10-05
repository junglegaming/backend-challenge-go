package composition

import (
	"context"
	"os"

	"go.uber.org/fx"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/config"
)

// Options is the whole application graph. main runs it, and the tests validate
// and start it, so all of them read the same wiring.
func Options() []fx.Option {
	return []fx.Option{
		ConfigModule,
		PersistenceModule,
		HTTPModule,
		ConsumerModule,
		fx.Invoke(requirePool),
		fx.Invoke(requireServer),
	}
}

// ConfigModule provides the configuration read from the environment.
var ConfigModule = fx.Module("config",
	fx.Provide(func() (config.Config, error) {
		return config.Load(os.LookupEnv)
	}),
)

// PersistenceModule provides the connection pool and the adapters built on it.
var PersistenceModule = fx.Module("persistence",
	fx.Provide(
		newPool,
		postgres.NewWalletRepository,
		postgres.NewWagerReader,
		postgres.NewWagerProcessor,
	),
)

// newPool opens the pool and registers its shutdown with the lifecycle. Fx
// stops hooks in reverse order of registration. The pool is built before the
// components that use it, so its hook runs after theirs. That order comes from
// the dependency graph, not from a manual list.
func newPool(lc fx.Lifecycle, cfg config.Config) (*postgres.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), postgres.PoolConfig{
		DSN:            cfg.DatabaseURL,
		MaxConns:       cfg.PoolMaxConns,
		AcquireTimeout: cfg.AcquireTimeout,
	})
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

// requirePool forces the pool to be built during startup. Without it, nothing
// would ask for the pool and a bad DSN would only show up on the first request.
func requirePool(*postgres.Pool) {}

// requireServer forces the HTTP server to be built, so its listener opens and
// its lifecycle hooks are registered.
func requireServer(*HTTPServer) {}
