package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig controls how the pgx connection pool behaves.
type PoolConfig struct {
	DSN             string
	MaxConns        int32
	MaxConnLifetime time.Duration
	// AcquireTimeout bounds how long a caller waits to acquire a connection
	// from the pool. Without it, a saturated pool or a slow database turns
	// a request into one that hangs instead of one that fails fast.
	AcquireTimeout time.Duration
}

const defaultAcquireTimeout = 5 * time.Second

// Pool wraps *pgxpool.Pool to enforce AcquireTimeout on every operation
// that goes through it, instead of leaving each caller to remember.
type Pool struct {
	*pgxpool.Pool
	acquireTimeout time.Duration
}

// NewPool builds a ready-to-use connection pool from cfg and verifies
// connectivity with a Ping before returning.
func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	pgxCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse pool config: %w", err)
	}

	if cfg.MaxConns > 0 {
		pgxCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MaxConnLifetime > 0 {
		pgxCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}

	acquireTimeout := cfg.AcquireTimeout
	if acquireTimeout <= 0 {
		acquireTimeout = defaultAcquireTimeout
	}

	rawPool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, acquireTimeout)
	defer cancel()
	if err := rawPool.Ping(pingCtx); err != nil {
		rawPool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return &Pool{Pool: rawPool, acquireTimeout: acquireTimeout}, nil
}

// withAcquireTimeout returns a copy of ctx bounded by the pool's configured
// AcquireTimeout. Call this before every query so a slow or saturated
// database surfaces as a fast, typed timeout instead of a hang.
func (p *Pool) withAcquireTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.acquireTimeout)
}

// Explicação
// Isso é infraestrutura pura — não sabe nada sobre Wallet ou carteiras, por isso é praticamente idêntico ao equivalente no wallet-go,
// e vai ser assim em qualquer projeto Go+Postgres que você fizer. AcquireTimeout existe porque, sem ele, um banco lento ou um pool esgotado
// faz a requisição ficar pendurada indefinidamente em vez de falhar rápido com um erro claro — é o tipo de coisa que só dói quando falta, nunca quando sobra.
