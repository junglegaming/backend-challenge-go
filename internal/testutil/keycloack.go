package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	keycloakImage = "quay.io/keycloak/keycloak:26.0"
	keycloakPort  = "8080/tcp"
	realmName     = "wallet"
)

// Keycloak is a running IdP with the wallet realm imported.
type Keycloak struct {
	// Issuer is the iss claim tokens carry: the base URL the tests reach the IdP on.
	Issuer    string
	container testcontainers.Container
}

// StartKeycloak runs Keycloak with deploy/keycloak/wallet-realm.json imported.
// It takes a context instead of testing.TB because TestMain has no *testing.T:
// the container is started once per package, not once per test.
func StartKeycloak(ctx context.Context) (*Keycloak, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        keycloakImage,
			ExposedPorts: []string{keycloakPort},
			Env: map[string]string{
				"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
				"KC_BOOTSTRAP_ADMIN_PASSWORD": "admin",
			},
			Cmd: []string{"start-dev", "--import-realm"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      realmFile(),
				ContainerFilePath: "/opt/keycloak/data/import/wallet-realm.json",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForHTTP("/realms/" + realmName).
				WithPort(keycloakPort).
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start keycloak: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("keycloak host: %w", err)
	}
	port, err := container.MappedPort(ctx, keycloakPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("keycloak port: %w", err)
	}

	return &Keycloak{
		Issuer:    fmt.Sprintf("http://%s:%s/realms/%s", host, port.Port(), realmName),
		container: container,
	}, nil
}

// Terminate stops and removes the container.
func (k *Keycloak) Terminate(ctx context.Context) error {
	return k.container.Terminate(ctx)
}

// JWKSURL is where the test's authenticator fetches the public keys.
func (k *Keycloak) JWKSURL() string {
	return k.Issuer + "/protocol/openid-connect/certs"
}

// FetchToken asks the IdP for a client_credentials access token, as a provider
// or the internal service would. A wrong secret comes back as an error.
func FetchToken(ctx context.Context, issuer, clientID, secret string) (string, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		issuer+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request for %s: %w", clientID, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token for %s: status %d: %s", clientID, resp.StatusCode, body)
	}

	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	return out.AccessToken, nil
}

// realmFile finds the realm next to the repository root, whichever package
// starts the container.
func realmFile() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "deploy", "keycloak", "wallet-realm.json")
}

// Explicação

// - StartKeycloak recebe context em vez de testing.TB. O TestMain do pacote não tem *testing.T, e o container deve subir uma vez por pacote, não por teste. Assim a espera de ~30 s acontece uma vez.
// - wait.ForHTTP("/realms/wallet") só libera quando o realm já está importado. Sem isso, o teste poderia pedir token antes de o client existir.
// - O Issuer usa a porta mapeada no host. Quando o token é pedido por localhost:<porta>, o Keycloak grava essa URL no iss. Por isso o issuer configurado no teste bate com o do token.
// - FetchToken usa client_credentials e devolve erro para qualquer status diferente de 200. Um secret errado vira erro explícito, e não um token vazio.
// - realmFile usa runtime.Caller, o mesmo truque do migrationsURL, para achar o arquivo independentemente do diretório de execução dos testes.
