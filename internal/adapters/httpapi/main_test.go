package httpapi

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

// testKeycloak is the IdP shared by every test in this package. It starts once,
// because booting Keycloak takes tens of seconds.
var (
	testKeycloak *testutil.Keycloak
	// testTokens holds one access token per client, keyed by client id.
	testTokens = map[string]string{}
)

// testSecrets are the development secrets from deploy/keycloak/wallet-realm.json.
var testSecrets = map[string]string{
	"provider-a":     "provider-a-dev-secret",
	"provider-b":     "provider-b-dev-secret",
	"wallet-service": "wallet-service-dev-secret",
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	kc, err := testutil.StartKeycloak(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start keycloak: %v\n", err)
		os.Exit(1)
	}
	testKeycloak = kc

	for client, secret := range testSecrets {
		token, err := testutil.FetchToken(ctx, kc.Issuer, client, secret)
		if err != nil {
			fmt.Fprintf(os.Stderr, "token for %s: %v\n", client, err)
			_ = kc.Terminate(ctx)
			os.Exit(1)
		}
		testTokens[client] = token
	}

	code := m.Run()
	_ = kc.Terminate(ctx)
	os.Exit(code)
}
