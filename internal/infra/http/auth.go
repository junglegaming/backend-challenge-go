package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

type providerIDKey struct{}

func WithProviderID(ctx context.Context, providerID string) context.Context {
	return context.WithValue(ctx, providerIDKey{}, providerID)
}

func ProviderIDFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(providerIDKey{}).(string)
	return v, ok
}

type AuthMiddleware struct {
	verifier *oidc.IDTokenVerifier
}

func NewAuthMiddleware(ctx context.Context, issuerURL string) (*AuthMiddleware, error) {
	if issuerURL == "" {
		return nil, fmt.Errorf("KEYCLOAK_ISSUER_URL is required for strict OIDC validation")
	}

	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to init oidc provider: %w", err)
	}

	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	return &AuthMiddleware{verifier: verifier}, nil
}

func (m *AuthMiddleware) RequireRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "missing or invalid authorization header", http.StatusUnauthorized)
			return
		}

		rawToken := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := m.verifier.Verify(r.Context(), rawToken)
		if err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		var claims struct {
			RealmAccess struct {
				Roles []string `json:"roles"`
			} `json:"realm_access"`
			ClientID string `json:"clientId"`
			Azp      string `json:"azp"`
		}

		if err := token.Claims(&claims); err != nil {
			http.Error(w, "failed to parse claims", http.StatusUnauthorized)
			return
		}

		providerID := claims.ClientID
		if providerID == "" {
			providerID = claims.Azp
		}

		if providerID == "" {
			http.Error(w, "missing client identification in token", http.StatusUnauthorized)
			return
		}

		hasRole := false
		for _, r := range claims.RealmAccess.Roles {
			if r == role {
				hasRole = true
				break
			}
		}

		if !hasRole {
			http.Error(w, "forbidden: missing role", http.StatusForbidden)
			return
		}

		ctx := WithProviderID(r.Context(), providerID)
		next(w, r.WithContext(ctx))
	}
}
