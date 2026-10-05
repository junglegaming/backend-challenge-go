package httpapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

// realAuthenticator verifies tokens against the Keycloak from TestMain, the way
// the production wiring does, with keys fetched over HTTP.
func realAuthenticator() *Authenticator {
	return NewAuthenticator(testKeycloak.Issuer, oidc.NewRemoteKeySet(context.Background(), testKeycloak.JWKSURL()))
}

func TestKeycloak_ProviderTokenIsAcceptedWithItsAZP(t *testing.T) {
	p, err := realAuthenticator().authenticate(bearerRequest(testTokens["provider-a"]))
	if err != nil {
		t.Fatalf("real token rejected: %v", err)
	}
	if p.ClientID != "provider-a" || p.isInternal() {
		t.Fatalf("principal = %+v, want provider-a and not internal", p)
	}
}

func TestKeycloak_InternalTokenCarriesTheRole(t *testing.T) {
	p, err := realAuthenticator().authenticate(bearerRequest(testTokens["wallet-service"]))
	if err != nil {
		t.Fatalf("real token rejected: %v", err)
	}
	if p.ClientID != "wallet-service" || !p.isInternal() {
		t.Fatalf("principal = %+v, want wallet-service with the internal role", p)
	}
}

func TestKeycloak_WrongSecretIssuesNoToken(t *testing.T) {
	_, err := testutil.FetchToken(context.Background(), testKeycloak.Issuer, "provider-a", "wrong-secret")
	if err == nil {
		t.Fatal("the IdP issued a token for a wrong secret")
	}
}

func TestKeycloak_ProviderTokenOnWalletRouteIs403(t *testing.T) {
	base := newTestAPI(t)

	status, out := call(t, http.MethodPost, base+"/wallets", bearer("provider-a"), map[string]any{})
	if status != http.StatusForbidden || errorCode(out) != "forbidden" {
		t.Fatalf("status %d code %q, want 403 forbidden", status, errorCode(out))
	}
}
