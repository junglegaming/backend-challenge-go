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
		// Mock verifier for testing without keycloak
		return &AuthMiddleware{verifier: nil}, nil
	}

	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to init oidc provider: %w", err)
	}
	// We skip client ID check because it's an API that accepts multiple clients
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

		var providerID string
		var roles []string

		if m.verifier == nil {
			// Mock logic if no verifier is provided (e.g. testing)
			// Assuming token is "mock-token:{role}:{providerId}"
			parts := strings.Split(rawToken, ":")
			if len(parts) >= 3 && parts[0] == "mock-token" {
				roles = append(roles, parts[1])
				providerID = parts[2]
			}
		} else {
			token, err := m.verifier.Verify(r.Context(), rawToken)
			if err != nil {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}

			var claims struct {
				RealmAccess struct {
					Roles []string `json:"roles"`
				} `json:"realm_access"`
				ClientID string `json:"clientId"` // Keycloak standard for client_credentials
			}

			if err := token.Claims(&claims); err != nil {
				http.Error(w, "failed to parse claims", http.StatusUnauthorized)
				return
			}

			roles = claims.RealmAccess.Roles
			providerID = claims.ClientID
		}

		hasRole := false
		for _, r := range roles {
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
