package composition

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

// TestLifecycle_StartServeStopReleasesResources runs the whole application
// against a real Postgres: it starts, answers a request, stops, and checks
// that the pool was released. This is the README §13 check on Fx start and
// stop.
func TestLifecycle_StartServeStopReleasesResources(t *testing.T) {
	t.Setenv("DATABASE_URL", testutil.StartPostgres(t))
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("AUTH_ISSUER", "http://localhost:1/realms/wallet")
	t.Setenv("AUTH_JWKS_URL", "http://localhost:1/realms/wallet/protocol/openid-connect/certs")
	t.Setenv("SQS_QUEUE_URL", "http://localhost:1/000000000000/wager-transactions.fifo")
	t.Setenv("SQS_ENDPOINT", "http://localhost:1")

	var (
		server *HTTPServer
		pool   *postgres.Pool
	)
	app := fx.New(append(Options(), fx.Populate(&server, &pool))...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp, err := http.Get("http://" + server.Addr() + "/health/live")
	if err != nil {
		t.Fatalf("GET /health/live while running: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health/live = %d, want 200", resp.StatusCode)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := pool.Ping(ctx); err == nil {
		t.Fatal("the pool is still open after Stop")
	}
}

// Explicação

// - A ordem de shutdown não está escrita em lugar nenhum. O servidor depende do processador, que depende do pool. O Fx para os
// componentes na ordem inversa do registro, então o Shutdown do servidor roda antes do Close do pool. Esse é o requisito do README:
// fechar dependências depois dos componentes que as usam.
// - newHTTPServer abre a porta no construtor, não no OnStart. Assim uma porta ocupada falha antes de qualquer componente subir, e o
// teste consegue ler o endereço real com HTTP_ADDR=127.0.0.1:0.
// - Se o Serve morrer sozinho, fx.Shutdowner encerra a aplicação com código 1. Sem isso, o processo ficaria de pé sem atender requisições.
// - OnStop usa SHUTDOWN_TIMEOUT em vez do contexto que o Fx passa. O prazo padrão do Fx é 15 s. Se alguém configurar SHUTDOWN_TIMEOUT acima
// disso, o Fx cortará antes. Isso fica registrado como próximo ajuste (fx.StopTimeout).
