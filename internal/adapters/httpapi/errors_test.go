package httpapi

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

func TestStatusFor(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"entrada invalida", fmt.Errorf("%w: x", errInvalidRequest), 400, "invalid_request"},
		{"valor invalido do dominio", domain.ErrInvalidAmount, 400, "invalid_request"},
		{"operacao invalida do dominio", domain.ErrInvalidWagerTransaction, 400, "invalid_request"},
		{"sem cabecalho de provedor", errUnauthenticated, 401, "unauthenticated"},
		{"carteira inexistente", postgres.ErrWalletNotFound, 404, "not_found"},
		{"operacao de outro provedor", errNotFound, 404, "not_found"},
		{"jogador nao e dono da carteira", postgres.ErrWalletOwnerMismatch, 404, "not_found"},
		{"chave reutilizada com corpo diferente", postgres.ErrIdempotencyKeyReuse, 422, "idempotency_key_reused"},
		{"chave ainda em processamento", postgres.ErrRequestInFlight, 409, "request_in_flight"},
		{"mesma operacao com outra chave", postgres.ErrDuplicateExternalTransaction, 409, "duplicate_operation"},
		{"carteira ja existe", postgres.ErrWalletAlreadyExists, 409, "conflict"},
		{"versao concorrente", postgres.ErrWalletVersionConflict, 409, "concurrent_update"},
		{"prazo do banco esgotado", fmt.Errorf("acquire: %w", context.DeadlineExceeded), 503, "temporarily_unavailable"},
		{"banco inacessivel (falha de conexao)", fmt.Errorf("begin tx: %w", &pgconn.ConnectError{}), 503, "temporarily_unavailable"},
		{"banco desligando (57P01)", fmt.Errorf("query: %w", &pgconn.PgError{Code: "57P01"}), 503, "temporarily_unavailable"},
		{"erro do banco que nao e indisponibilidade (23514)", fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23514"}), 500, "internal"},
		{"erro desconhecido", errors.New("boom"), 500, "internal"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, code := statusFor(tc.err)
			if status != tc.status || code != tc.code {
				t.Fatalf("statusFor = %d %s, want %d %s", status, code, tc.status, tc.code)
			}
		})
	}
}
