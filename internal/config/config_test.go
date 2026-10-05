package config

import (
	"errors"
	"testing"
	"time"
)

func envOf(m map[string]string) Lookup {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

// validEnv is the smallest environment Load accepts. Each subtest removes or
// changes one key from it.
func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":  "postgres://x",
		"AUTH_ISSUER":   "http://localhost:8081/realms/wallet",
		"AUTH_JWKS_URL": "http://localhost:8081/realms/wallet/protocol/openid-connect/certs",
		"SQS_QUEUE_URL": "http://localhost:4566/000000000000/wager-transactions.fifo",
	}
}

func TestLoad(t *testing.T) {
	t.Run("sem_database_url_falha", func(t *testing.T) {
		env := validEnv()
		delete(env, "DATABASE_URL")
		if _, err := Load(envOf(env)); !errors.Is(err, ErrMissingDatabaseURL) {
			t.Fatalf("err = %v, want ErrMissingDatabaseURL", err)
		}
	})

	t.Run("sem_auth_issuer_falha", func(t *testing.T) {
		env := validEnv()
		delete(env, "AUTH_ISSUER")
		if _, err := Load(envOf(env)); !errors.Is(err, ErrMissingAuthIssuer) {
			t.Fatalf("err = %v, want ErrMissingAuthIssuer", err)
		}
	})

	t.Run("sem_auth_jwks_url_falha", func(t *testing.T) {
		env := validEnv()
		delete(env, "AUTH_JWKS_URL")
		if _, err := Load(envOf(env)); !errors.Is(err, ErrMissingAuthJWKSURL) {
			t.Fatalf("err = %v, want ErrMissingAuthJWKSURL", err)
		}
	})

	t.Run("aplica_padroes", func(t *testing.T) {
		cfg, err := Load(envOf(validEnv()))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.PoolMaxConns != 10 || cfg.AcquireTimeout != 5*time.Second || cfg.HTTPAddr != ":8080" {
			t.Fatalf("defaults = %+v", cfg)
		}
		if cfg.ShutdownTimeout != 10*time.Second {
			t.Fatalf("ShutdownTimeout = %s, want 10s", cfg.ShutdownTimeout)
		}
		if cfg.AuthIssuer != "http://localhost:8081/realms/wallet" {
			t.Fatalf("AuthIssuer = %q", cfg.AuthIssuer)
		}
	})

	t.Run("rejeita_pool_zero", func(t *testing.T) {
		env := validEnv()
		env["DB_MAX_CONNS"] = "0"
		if _, err := Load(envOf(env)); err == nil {
			t.Fatal("DB_MAX_CONNS=0 must be rejected")
		}
	})

	t.Run("rejeita_timeout_invalido", func(t *testing.T) {
		env := validEnv()
		env["DB_ACQUIRE_TIMEOUT"] = "cinco"
		if _, err := Load(envOf(env)); err == nil {
			t.Fatal("unparseable DB_ACQUIRE_TIMEOUT must be rejected")
		}
	})

	t.Run("rejeita_shutdown_negativo", func(t *testing.T) {
		env := validEnv()
		env["SHUTDOWN_TIMEOUT"] = "-1s"
		if _, err := Load(envOf(env)); err == nil {
			t.Fatal("negative SHUTDOWN_TIMEOUT must be rejected")
		}
	})

	t.Run("sem_sqs_queue_url_falha", func(t *testing.T) {
		env := validEnv()
		delete(env, "SQS_QUEUE_URL")
		if _, err := Load(envOf(env)); !errors.Is(err, ErrMissingSQSQueueURL) {
			t.Fatalf("err = %v, want ErrMissingSQSQueueURL", err)
		}
	})
}

// Explicação

// - AuthIssuer e AuthJWKSURL são dois campos porque são dois papéis diferentes. O issuer é o que o token declara no iss. A URL do JWKS é de onde o app baixa as chaves. Dentro do compose, o token traz localhost:8081 mas o app acessa keycloak:8080. Se fossem a mesma coisa, a validação falharia.
// - Sem as duas variáveis, a API não sobe. Isso segue a regra do README: sem autenticação efetiva, o serviço não deve aceitar tráfego.
