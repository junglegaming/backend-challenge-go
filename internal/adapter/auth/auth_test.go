package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
)

const (
	issuer   = "http://idp.test/realms/wallet"
	audience = "wallet-api"
)

// idp is a local OIDC key server that signs tokens like Keycloak does. Its
// key set can be rotated and its JWKS endpoint counts hits and can fail.
type idp struct {
	server  *httptest.Server
	hits    atomic.Int64
	fail    atomic.Bool
	garbage atomic.Bool

	mu   sync.Mutex
	keys map[string]*rsa.PrivateKey
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	i := &idp{keys: map[string]*rsa.PrivateKey{"k1": newKey(t)}}
	i.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i.hits.Add(1)
		if i.fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if i.garbage.Load() {
			_, _ = w.Write([]byte("<html>not a key set</html>"))
			return
		}
		_ = json.NewEncoder(w).Encode(i.jwks())
	}))
	t.Cleanup(i.server.Close)
	return i
}

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func (i *idp) jwks() jose.JSONWebKeySet {
	i.mu.Lock()
	defer i.mu.Unlock()
	var set jose.JSONWebKeySet
	for kid, key := range i.keys {
		set.Keys = append(set.Keys, jose.JSONWebKey{Key: &key.PublicKey, KeyID: kid, Algorithm: "RS256", Use: "sig"})
	}
	return set
}

// rotate replaces every signing key with a single new one.
func (i *idp) rotate(t *testing.T, kid string) *rsa.PrivateKey {
	t.Helper()
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys = map[string]*rsa.PrivateKey{kid: newKey(t)}
	return i.keys[kid]
}

// sign issues an RS256 token; kid is the header key id ("" omits it).
func (i *idp) sign(t *testing.T, kid string, claims map[string]any) string {
	t.Helper()
	i.mu.Lock()
	key := i.keys[kid]
	i.mu.Unlock()
	if key == nil {
		key = newKey(t)
	}
	return signWith(t, jose.SigningKey{Algorithm: jose.RS256, Key: key}, kid, claims)
}

func signWith(t *testing.T, sk jose.SigningKey, kid string, claims map[string]any) string {
	t.Helper()
	opts := (&jose.SignerOptions{}).WithType("JWT")
	if kid != "" {
		opts = opts.WithHeader("kid", kid)
	}
	signer, err := jose.NewSigner(sk, opts)
	require.NoError(t, err)
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)
	return raw
}

func (i *idp) verifier(interval time.Duration) *auth.Verifier {
	return auth.NewVerifier(context.Background(), auth.Config{
		Issuer: issuer, JWKSURL: i.server.URL, Audience: audience, JWKSRefreshInterval: interval,
	})
}

func claims(overrides map[string]any) map[string]any {
	c := map[string]any{
		"iss": issuer, "aud": []string{audience, "account"}, "sub": "service-account-provider-a",
		"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "azp": "provider-a",
		"provider_id":  "provider-a",
		"realm_access": map[string]any{"roles": []string{auth.RoleProvider, "offline_access"}},
	}
	for k, v := range overrides {
		c[k] = v
	}
	return c
}

func TestVerifyValidProviderToken(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	p, err := i.verifier(0).Verify(context.Background(), i.sign(t, "k1", claims(nil)))
	require.NoError(t, err)
	assert.Equal(t, "provider-a", p.ProviderID)
	assert.Equal(t, "provider-a", p.ClientID)
	assert.Equal(t, "service-account-provider-a", p.Subject)
	assert.True(t, p.IsProvider())
	assert.False(t, p.IsInternal())
}

func TestVerifyInternalToken(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	raw := i.sign(t, "k1", claims(map[string]any{"provider_id": nil, "realm_access": map[string]any{"roles": []string{auth.RoleWalletAdmin}}}))
	p, err := i.verifier(0).Verify(context.Background(), raw)
	require.NoError(t, err)
	assert.True(t, p.IsInternal())
	assert.False(t, p.IsProvider())
}

// unsignedToken serializes a JWT with alg=none.
func unsignedToken(t *testing.T, c map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	require.NoError(t, err)
	payload, err := json.Marshal(c)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
}

func TestVerifyRejectsInvalidTokens(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	hmac := jose.SigningKey{Algorithm: jose.HS256, Key: []byte("0123456789abcdef0123456789abcdef")}
	cases := []struct {
		name string
		raw  string
	}{
		{"malformed", "not-a-jwt"},
		{"alg none", unsignedToken(t, claims(nil))},
		{"HS256 signed", signWith(t, hmac, "k1", claims(nil))},
		{"missing kid", i.sign(t, "", claims(nil))},
		{"unknown kid", i.sign(t, "k-unknown", claims(nil))},
		{"expired", i.sign(t, "k1", claims(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()}))},
		{"missing exp", i.sign(t, "k1", without(claims(nil), "exp"))},
		{"future nbf beyond leeway", i.sign(t, "k1", claims(map[string]any{"nbf": time.Now().Add(10 * time.Minute).Unix()}))},
		{"wrong issuer", i.sign(t, "k1", claims(map[string]any{"iss": "http://evil"}))},
		{"wrong audience", i.sign(t, "k1", claims(map[string]any{"aud": "other-api"}))},
		{"audience list without api", i.sign(t, "k1", claims(map[string]any{"aud": []string{"account", "other-api"}}))},
		{"bad signature", signWith(t, jose.SigningKey{Algorithm: jose.RS256, Key: newKey(t)}, "k1", claims(nil))},
		{"bad claims", i.sign(t, "k1", claims(map[string]any{"realm_access": "admin"}))},
		{"provider_id not a string", i.sign(t, "k1", claims(map[string]any{"provider_id": 42}))},
	}
	v := i.verifier(0)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), tc.raw)
			assert.ErrorIs(t, err, auth.ErrUnauthenticated)
		})
	}
}

func without(c map[string]any, key string) map[string]any {
	delete(c, key)
	return c
}

func TestVerifyAcceptsNbfWithinLeeway(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	raw := i.sign(t, "k1", claims(map[string]any{"nbf": time.Now().Add(time.Minute).Unix()}))
	_, err := i.verifier(0).Verify(context.Background(), raw)
	assert.NoError(t, err, "go-oidc allows 5 minutes of clock skew on nbf")
}

func TestKeyRotationRefreshesJWKS(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	v := i.verifier(time.Millisecond)
	ctx := context.Background()
	old := i.sign(t, "k1", claims(nil))
	_, err := v.Verify(ctx, old)
	require.NoError(t, err)
	assert.EqualValues(t, 1, i.hits.Load(), "first token fetches the keys")

	_, err = v.Verify(ctx, i.sign(t, "k1", claims(nil)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, i.hits.Load(), "cached key: no refetch")

	i.rotate(t, "k2")
	time.Sleep(5 * time.Millisecond)
	_, err = v.Verify(ctx, i.sign(t, "k2", claims(nil)))
	require.NoError(t, err, "new kid triggers a refresh")
	assert.EqualValues(t, 2, i.hits.Load())

	time.Sleep(5 * time.Millisecond)
	_, err = v.Verify(ctx, old)
	assert.ErrorIs(t, err, auth.ErrUnauthenticated, "tokens signed with the retired key are rejected")
}

func TestUnknownKidRefreshIsRateLimited(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	v := i.verifier(time.Hour)
	ctx := context.Background()
	_, err := v.Verify(ctx, i.sign(t, "k1", claims(nil)))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for n := range 50 {
		wg.Go(func() {
			raw := i.sign(t, "kid-"+string(rune('a'+n%26))+"-random", claims(nil))
			_, err := v.Verify(ctx, raw)
			assert.ErrorIs(t, err, auth.ErrUnauthenticated)
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, i.hits.Load(), int64(2), "forged key ids refresh the JWKS at most once per interval")

	_, err = v.Verify(ctx, i.sign(t, "k1", claims(nil)))
	assert.NoError(t, err, "known keys keep working while refreshes are throttled")
}

func TestColdCacheServesConcurrentTokensWithOneFetch(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	v := i.verifier(time.Hour)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, err := v.Verify(ctx, i.sign(t, "k1", claims(nil)))
			assert.NoError(t, err, "callers queued behind the first fetch reuse its keys")
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, i.hits.Load(), "concurrent misses share one fetch")
}

func TestJWKSServerErrorIsUnauthenticated(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	i.fail.Store(true)
	_, err := i.verifier(0).Verify(context.Background(), i.sign(t, "k1", claims(nil)))
	assert.ErrorIs(t, err, auth.ErrUnauthenticated)
	assert.NotErrorIs(t, err, auth.ErrJWKSUnavailable, "go-oidc keeps key-set diagnostics behind the public auth boundary")
	assert.EqualValues(t, 1, i.hits.Load())
}

func TestJWKSUnreadableIsUnauthenticated(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	i.garbage.Store(true)
	_, err := i.verifier(0).Verify(context.Background(), i.sign(t, "k1", claims(nil)))
	assert.ErrorIs(t, err, auth.ErrUnauthenticated, "a JWKS document that is not JSON never verifies a token")

	broken := auth.NewVerifier(context.Background(), auth.Config{Issuer: issuer, JWKSURL: "://not-a-url", Audience: audience})
	_, err = broken.Verify(context.Background(), i.sign(t, "k1", claims(nil)))
	assert.ErrorIs(t, err, auth.ErrUnauthenticated, "an unusable JWKS URL fails closed")
}

func TestJWKSTimeoutIsBounded(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { <-release }))
	t.Cleanup(func() { close(release); slow.Close() })
	i := newIDP(t)
	v := auth.NewVerifier(context.Background(), auth.Config{Issuer: issuer, JWKSURL: slow.URL, Audience: audience, JWKSTimeout: 50 * time.Millisecond})
	start := time.Now()
	_, err := v.Verify(context.Background(), i.sign(t, "k1", claims(nil)))
	assert.ErrorIs(t, err, auth.ErrUnauthenticated)
	assert.Less(t, time.Since(start), 2*time.Second, "the JWKS fetch is bounded by JWKSTimeout")
}

func TestPrincipalRoles(t *testing.T) {
	t.Parallel()
	p := auth.Principal{Roles: []string{auth.RoleProvider}}
	assert.True(t, p.HasRole(auth.RoleProvider))
	assert.False(t, p.IsProvider(), "a provider role without providerId is not a provider")
}
