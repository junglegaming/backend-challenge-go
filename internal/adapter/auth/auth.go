// Package auth validates OAuth 2.0 access tokens issued by an external OIDC
// provider (Keycloak) and maps them to principals.
package auth

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
)

// Realm roles granted by the IdP.
const (
	// RoleProvider allows submitting and reading the provider's own operations.
	RoleProvider = "wager-provider"
	// RoleWalletAdmin is held by the internal service; it allows wallet
	// operations and reading every transaction.
	RoleWalletAdmin = "wallet-admin"
)

// ErrUnauthenticated reports a missing, malformed, invalid or expired token.
var ErrUnauthenticated = errors.New("unauthenticated")

// ErrJWKSUnavailable classifies failures to refresh the remote signing keys.
// go-oidc deliberately converts key-set errors to text, so this sentinel is
// available to the auth adapter's internal refresh paths but is not exposed by
// Verifier.Verify; callers continue to receive ErrUnauthenticated.
var ErrJWKSUnavailable = errors.New("JWKS unavailable")

// Principal is the authenticated caller.
type Principal struct {
	// Subject is the "sub" claim.
	Subject string
	// ClientID is the OAuth client that obtained the token ("azp").
	ClientID string
	// ProviderID binds a provider client to its providerId; empty otherwise.
	ProviderID string
	// Roles are the realm roles granted by the IdP.
	Roles []string
}

// HasRole reports whether the principal holds a realm role.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// IsInternal reports whether the caller is the internal wallet service.
func (p Principal) IsInternal() bool { return p.HasRole(RoleWalletAdmin) }

// IsProvider reports whether the caller is a game provider bound to a
// providerId. The providerId comes only from the token, never from input.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// Config configures token validation.
type Config struct {
	// Issuer is the expected "iss" claim.
	Issuer string
	// JWKSURL is where signing keys are fetched (may differ from the issuer
	// host inside Docker networks).
	JWKSURL string
	// Audience is the expected "aud" entry for this API.
	Audience string
	// JWKSTimeout bounds each JWKS fetch; DefaultJWKSTimeout when zero.
	JWKSTimeout time.Duration
	// JWKSRefreshInterval is the minimum time between two JWKS fetches
	// triggered by tokens that match no cached key; tokens arriving within
	// the interval fail fast. DefaultJWKSRefreshInterval when zero.
	JWKSRefreshInterval time.Duration
}

// Defaults of the JWKS client.
const (
	// DefaultJWKSTimeout is the JWKS fetch timeout when none is configured.
	DefaultJWKSTimeout = 5 * time.Second
	// DefaultJWKSRefreshInterval is the minimum spacing of cache-miss
	// refreshes when none is configured.
	DefaultJWKSRefreshInterval = 30 * time.Second
)

// Verifier validates access tokens.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewVerifier builds a verifier backed by the remote JWKS. Keys are fetched
// lazily with a bounded timeout and cached; a token matching no cached key
// refreshes them at most once per Config.JWKSRefreshInterval.
func NewVerifier(_ context.Context, cfg Config) *Verifier {
	fetchTimeout := cmp.Or(cfg.JWKSTimeout, DefaultJWKSTimeout)
	keys := &keySet{
		url:          cfg.JWKSURL,
		client:       &http.Client{Timeout: fetchTimeout},
		fetchTimeout: fetchTimeout,
		interval:     cmp.Or(cfg.JWKSRefreshInterval, DefaultJWKSRefreshInterval),
		algs:         []jose.SignatureAlgorithm{jose.RS256},
	}
	return &Verifier{verifier: oidc.NewVerifier(cfg.Issuer, keys, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})}
}

type claims struct {
	AuthorizedParty string `json:"azp"`
	ProviderID      string `json:"provider_id"`
	RealmAccess     struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify checks signature, issuer, audience and expiry, then extracts the
// principal. Every failure wraps ErrUnauthenticated; the cause is safe to
// log but never to return to clients.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	return Principal{Subject: tok.Subject, ClientID: c.AuthorizedParty, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, nil
}
