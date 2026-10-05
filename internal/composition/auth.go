package composition

import (
	"context"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/httpapi"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/config"
	"github.com/coreos/go-oidc/v3/oidc"
)

// newAuthenticator builds the token verifier. The keys are fetched on the first
// request, not at startup, so the API boots even while the IdP is unreachable.
func newAuthenticator(cfg config.Config) *httpapi.Authenticator {
	keys := oidc.NewRemoteKeySet(context.Background(), cfg.AuthJWKSURL)
	return httpapi.NewAuthenticator(cfg.AuthIssuer, keys)
}
