package composition

import (
	"testing"

	"go.uber.org/fx"
)

// TestGraphValidates checks that every dependency is provided and that the
// constructors are wired correctly. It does not run any constructor, so it
// needs no database and no environment variables.
func TestGraphValidates(t *testing.T) {
	if err := fx.ValidateApp(Options()...); err != nil {
		t.Fatalf("fx graph is invalid: %v", err)
	}
}

// Explicação

// - fx.ValidateApp só checa o grafo: dependências faltando, ciclos, tipos incompatíveis. Não executa construtores nem OnStart,
// por isso o teste roda sem Docker e sem DATABASE_URL.
// - Ele não substitui um teste de início e encerramento com Postgres real. Esse teste (fx.New, app.Start, app.Stop com container)
// é o próximo passo depois de existir o servidor HTTP, porque o encerramento só fica observável com algo para parar.
