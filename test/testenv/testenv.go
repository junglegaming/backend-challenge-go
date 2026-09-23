//go:build integration || e2e

// Package testenv starts the real dependencies of the service in containers
// (PostgreSQL, LocalStack SQS and Keycloak) for integration and e2e tests.
package testenv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	"github.com/Pantani/backend-challenge-go/internal/config"
)

// Images used by the tests (same as docker-compose.yml).
const (
	PostgresImage   = "postgres:17-alpine"
	LocalStackImage = "localstack/localstack:4.4"
	KeycloakImage   = "quay.io/keycloak/keycloak:26.3"
)

// Clients holds the client credentials provisioned by deploy/keycloak/realm-wallet.json.
var Clients = map[string]string{
	"provider-a":             "provider-a-secret",
	"provider-b":             "provider-b-secret",
	"wallet-service":         "wallet-service-secret",
	"provider-a-short-lived": "provider-a-short-lived-secret",
	"no-role-client":         "no-role-client-secret",
}

// Runtime login created by deploy/postgres/init (member of wallet_app).
const (
	RuntimeUser     = "wallet_service"
	RuntimePassword = "wallet_service"
)

// Env holds the endpoints of the running dependencies. DatabaseURL is the
// least-privilege runtime login the service uses, as in production;
// OwnerDatabaseURL is the schema owner, for migrations and administration.
type Env struct {
	DatabaseURL      string
	OwnerDatabaseURL string
	SQSEndpoint      string
	KeycloakURL      string
	mu               sync.Mutex
	containers       []testcontainers.Container
	root             string
}

func (e *Env) track(c testcontainers.Container) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.containers = append(e.containers, c)
}

// Issuer is the expected token issuer.
func (e *Env) Issuer() string { return e.KeycloakURL + "/realms/wallet" }

// JWKSURL is where signing keys are published.
func (e *Env) JWKSURL() string { return e.Issuer() + "/protocol/openid-connect/certs" }

// RepoRoot finds the module root (the directory holding go.mod).
func RepoRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve module directory: %w", err)
	}
	for {
		if info, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !info.IsDir() {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", start)
		}
		dir = parent
	}
}

// Start launches the three containers in parallel. AWS credentials for
// LocalStack are exported to the process environment.
func Start(ctx context.Context) (*Env, error) {
	root, err := RepoRoot(".")
	if err != nil {
		return nil, err
	}
	if err := setAWSVariables(); err != nil {
		return nil, err
	}
	env := &Env{root: root}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return env.startPostgres(gctx) })
	g.Go(func() error { return env.startLocalStack(gctx) })
	g.Go(func() error { return env.startKeycloak(gctx) })
	if err := g.Wait(); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		return nil, errors.Join(err, env.Stop(cleanup))
	}
	return env, nil
}

func setAWSVariables() error {
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": "us-east-1"} {
		if err := os.Setenv(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stop terminates the containers.
func (e *Env) Stop(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var errs []error
	for _, c := range e.containers {
		errs = append(errs, c.Terminate(ctx))
	}
	return errors.Join(errs...)
}

func (e *Env) startPostgres(ctx context.Context) error {
	c, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("wallet"), tcpostgres.WithUsername("wallet"), tcpostgres.WithPassword("wallet"),
		tcpostgres.WithInitScripts(filepath.Join(e.root, "deploy", "postgres", "init", "01-runtime-user.sql")),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres", "-c", "max_connections=300"))
	if c != nil {
		e.track(c)
	}
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if e.OwnerDatabaseURL, err = c.ConnectionString(ctx, "sslmode=disable"); err != nil {
		return err
	}
	e.DatabaseURL, err = WithUser(e.OwnerDatabaseURL, RuntimeUser, RuntimePassword)
	return err
}

// WithUser returns databaseURL authenticated as another user.
func WithUser(databaseURL, user, password string) (string, error) {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(user, password)
	return u.String(), nil
}

func (e *Env) startLocalStack(ctx context.Context) error {
	c, err := localstack.Run(ctx, LocalStackImage, testcontainers.WithEnv(map[string]string{"SERVICES": "sqs"}))
	if c != nil {
		e.track(c)
	}
	if err != nil {
		return fmt.Errorf("localstack: %w", err)
	}
	e.SQSEndpoint, err = c.PortEndpoint(ctx, "4566/tcp", "http")
	return err
}

func (e *Env) startKeycloak(ctx context.Context) error {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        KeycloakImage,
			Cmd:          []string{"start-dev", "--import-realm", "--health-enabled=true"},
			Env:          map[string]string{"KC_BOOTSTRAP_ADMIN_USERNAME": "admin", "KC_BOOTSTRAP_ADMIN_PASSWORD": "admin"},
			ExposedPorts: []string{"8080/tcp", "9000/tcp"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      filepath.Join(e.root, "deploy", "keycloak", "realm-wallet.json"),
				ContainerFilePath: "/opt/keycloak/data/import/realm-wallet.json", FileMode: 0o644,
			}},
			WaitingFor: wait.ForHTTP("/health/ready").WithPort("9000/tcp").WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		e.track(c)
	}
	if err != nil {
		return fmt.Errorf("keycloak: %w", err)
	}
	e.KeycloakURL, err = c.PortEndpoint(ctx, "8080/tcp", "http")
	e.KeycloakURL = strings.Replace(e.KeycloakURL, "127.0.0.1", "localhost", 1)
	return err
}

// Token obtains an access token with the client_credentials grant.
func (e *Env) Token(ctx context.Context, clientID string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {Clients[clientID]}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Issuer()+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", tokenStatusError(resp)
	}
	return decodeToken(resp.Body)
}

// Only OAuth error codes are safe to disclose: arbitrary upstream bodies may
// contain credentials or tokens, including values unknown to this client.
func tokenStatusError(resp *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil {
		return fmt.Errorf("keycloak HTTP %d: read error body: %w", resp.StatusCode, err)
	}
	var body struct {
		Error string `json:"error"`
	}
	excerpt := "[redacted response body]"
	if json.Unmarshal(data, &body) == nil {
		switch body.Error {
		case "invalid_client", "invalid_grant", "invalid_request", "unauthorized_client", "unsupported_grant_type":
			excerpt = body.Error
		}
	}
	return fmt.Errorf("keycloak HTTP %d: %s", resp.StatusCode, excerpt)
}

func decodeToken(r io.Reader) (string, error) {
	var body struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error_description"`
	}
	if err := json.NewDecoder(r).Decode(&body); err != nil {
		return "", err
	}
	if body.AccessToken == "" {
		return "", errors.New("keycloak: " + body.Error)
	}
	return body.AccessToken, nil
}

// Vars returns the environment variables that point the service at the
// containers; overrides replace or add variables.
func (e *Env) Vars(overrides map[string]string) map[string]string {
	vars := map[string]string{
		"DATABASE_URL": e.DatabaseURL, "AWS_ENDPOINT_URL": e.SQSEndpoint, "AWS_REGION": "us-east-1",
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
		"OIDC_ISSUER": e.Issuer(), "OIDC_JWKS_URL": e.JWKSURL(), "OIDC_AUDIENCE": "wallet-api",
		"HTTP_ADDR": "127.0.0.1:0", "LOG_LEVEL": "warn",
		"SQS_WAIT_TIME": "1s", "SQS_VISIBILITY_TIMEOUT": "1m", "SQS_PROCESS_TIMEOUT": "4s", "SQS_ACK_TIMEOUT": "500ms",
		"SQS_RETRY_BASE": "1s", "SQS_RETRY_MAX": "2s", "SQS_MAX_RECEIVE_COUNT": "3",
		"PENDING_INTERVAL": "100ms", "PENDING_BASE_DELAY": "200ms", "PENDING_MAX_DELAY": "1s", "PENDING_MAX_ATTEMPTS": "5",
		"OUTBOX_INTERVAL": "100ms", "OUTBOX_LEASE": "2s", "OUTBOX_PUBLISH_TIMEOUT": "1s", "OUTBOX_FINALIZE_TIMEOUT": "500ms",
		"OUTBOX_RETRY_BASE": "200ms", "OUTBOX_RETRY_MAX": "1s",
		"SHUTDOWN_TIMEOUT": "61s",
	}
	for k, v := range overrides {
		vars[k] = v
	}
	return vars
}

// Config loads a service configuration pointing at the containers.
func (e *Env) Config(overrides map[string]string) (config.Config, error) {
	return config.Load(config.MapLookup(e.Vars(overrides)))
}
