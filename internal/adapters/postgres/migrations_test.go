package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const migrationsDir = "file://../../../migrations"

func startDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("wagering_test"),
		tcpostgres.WithUsername("wagering"),
		tcpostgres.WithPassword("wagering"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	return dsn
}

func migrateTo(t *testing.T, dsn string, op func(*migrate.Migrate) error) {
	t.Helper()
	m, err := migrate.New(migrationsDir, dsn)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := op(m); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate: %v", err)
	}
}

// expectRejected asserts that a statement fails. The database, not the test,
// is the one deciding — a nil error means the rule is missing.
func expectRejected(t *testing.T, conn *pgx.Conn, name, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err == nil {
		t.Errorf("%s: database accepted it, rule is missing", name)
	}
}

func TestMigrations_UpEnforceInvariants(t *testing.T) {
	dsn := startDB(t)
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Up() })

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	walletID := uuid.New()
	if _, err := conn.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance_minor, version) VALUES ($1, $2, 'BRL', 10000, 1)`,
		walletID, uuid.New()); err != nil {
		t.Fatalf("seed wallet: %v", err)
	}

	insertWager := func(id uuid.UUID, ext, kind, state string, amount int64, ref *uuid.UUID) error {
		_, err := conn.Exec(ctx, `
                      INSERT INTO wager_transactions
                       (id, provider_id, external_transaction_id, idempotency_key, request_hash, wallet_id, player_id,
                        round_id, game_id, kind, amount_minor, currency, reference_transaction_id, state)
                      VALUES ($1, 'provider-a', $2, 'k-'||$2, 'h', $3, $4, 'round', 'game', $5, $6, 'BRL', $7, $8)`,
			id, ext, walletID, uuid.New(), kind, amount, ref, state)
		return err
	}

	bet := uuid.New()
	if err := insertWager(bet, "bet-1", "BET", "PROCESSED", 2500, nil); err != nil {
		t.Fatalf("valid BET rejected: %v", err)
	}

	t.Run("duplicata_do_provedor_e_rejeitada", func(t *testing.T) {
		if err := insertWager(uuid.New(), "bet-1", "BET", "PENDING", 2500, nil); err == nil {
			t.Error("duplicate (provider, external id) accepted")
		}
	})

	t.Run("loss_com_valor_e_rejeitado", func(t *testing.T) {
		if err := insertWager(uuid.New(), "loss-1", "LOSS", "PROCESSED", 100, nil); err == nil {
			t.Error("LOSS with non-zero amount accepted")
		}
	})

	t.Run("bet_com_valor_zero_e_rejeitado", func(t *testing.T) {
		if err := insertWager(uuid.New(), "bet-zero", "BET", "PENDING", 0, nil); err == nil {
			t.Error("BET with zero amount accepted")
		}
	})

	t.Run("segunda_reversao_processada_e_rejeitada", func(t *testing.T) {
		if err := insertWager(uuid.New(), "refund-1", "REFUND", "PROCESSED", 2500, &bet); err != nil {
			t.Fatalf("first refund rejected: %v", err)
		}
		if err := insertWager(uuid.New(), "rollback-1", "ROLLBACK", "PROCESSED", 2500, &bet); err == nil {
			t.Error("second successful reversal of the same bet accepted")
		}
	})

	t.Run("chave_completed_sem_resposta_e_rejeitada", func(t *testing.T) {
		expectRejected(t, conn, "completed without response",
			`INSERT INTO idempotency_keys (scope, endpoint, key, request_hash, state) VALUES ('s','POST /t','k1','h','completed')`)
	})

	// A row-level trigger only fires for rows that exist, so each ledger test
	// seeds its own entry instead of depending on an earlier subtest.
	seedLedgerEntry := func(t *testing.T) {
		t.Helper()
		if _, err := conn.Exec(ctx,
			`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor)
			 VALUES ($1, $2, $3, 'DEBIT', 100, 10000, 9900)`,
			uuid.New(), walletID, uuid.New()); err != nil {
			t.Fatalf("seed ledger entry: %v", err)
		}
	}

	t.Run("ledger_nao_aceita_update", func(t *testing.T) {
		seedLedgerEntry(t)
		expectRejected(t, conn, "ledger update", `UPDATE wallet_ledger_entries SET amount_minor = 1`)
	})

	t.Run("ledger_nao_aceita_delete", func(t *testing.T) {
		seedLedgerEntry(t)
		expectRejected(t, conn, "ledger delete", `DELETE FROM wallet_ledger_entries`)
	})

	t.Run("ledger_nao_aceita_truncate", func(t *testing.T) {
		expectRejected(t, conn, "ledger truncate", `TRUNCATE wallet_ledger_entries`)
	})

	t.Run("inbox_dedup_por_consumidor_e_mensagem", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `INSERT INTO inbox (consumer_name, message_id, request_hash) VALUES ('c','m1','h')`); err != nil {
			t.Fatalf("first inbox row: %v", err)
		}
		expectRejected(t, conn, "duplicate inbox message", `INSERT INTO inbox (consumer_name, message_id, request_hash) VALUES ('c','m1','h')`)
	})
}

func TestMigrations_DownThenUpAgain(t *testing.T) {
	dsn := startDB(t)
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Up() })
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Down() })
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Down() })
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Up() })
	migrateTo(t, dsn, func(m *migrate.Migrate) error { return m.Up() })
}
