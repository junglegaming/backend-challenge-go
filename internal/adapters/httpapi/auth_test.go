package httpapi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const testIssuer = "https://issuer.test/realms/wallet"

func newTestKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func rs256(key *rsa.PrivateKey) jose.SigningKey {
	return jose.SigningKey{Algorithm: jose.RS256, Key: key}
}

// signWith signs claims the way the IdP does, with the given key.
func signWith(t *testing.T, sk jose.SigningKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(sk, nil)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatalf("serialize token: %v", err)
	}
	return token
}

// tokenClaims are the claims a valid Keycloak access token carries.
func tokenClaims(azp string, exp time.Time, roles ...string) map[string]any {
	return map[string]any{
		"iss":          testIssuer,
		"aud":          "account",
		"azp":          azp,
		"iat":          time.Now().Unix(),
		"exp":          exp.Unix(),
		"realm_access": map[string]any{"roles": roles},
	}
}

func newTestAuthenticator(key *rsa.PrivateKey) *Authenticator {
	keys := &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{&key.PublicKey}}
	return NewAuthenticator(testIssuer, keys)
}

func bearerRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestAuthenticate(t *testing.T) {
	key := newTestKey(t)
	other := newTestKey(t)
	auth := newTestAuthenticator(key)

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	wrongIssuer := tokenClaims("provider-a", future)
	wrongIssuer["iss"] = "https://evil.test/realms/wallet"

	noAZP := tokenClaims("", future)
	delete(noAZP, "azp")

	cases := []struct {
		name  string
		token string
		ok    bool
	}{
		{"token_de_provedor_valido", signWith(t, rs256(key), tokenClaims("provider-a", future)), true},
		{"token_interno_valido", signWith(t, rs256(key), tokenClaims("wallet-service", future, "wallet-internal")), true},
		{"expirado", signWith(t, rs256(key), tokenClaims("provider-a", past)), false},
		{"issuer_de_outro_realm", signWith(t, rs256(key), wrongIssuer), false},
		{"assinado_por_outra_chave", signWith(t, rs256(other), tokenClaims("provider-a", future)), false},
		{"algoritmo_hmac", signWith(t, jose.SigningKey{Algorithm: jose.HS256, Key: []byte("0123456789abcdef0123456789abcdef")}, tokenClaims("provider-a", future)), false},
		{"sem_azp", signWith(t, rs256(key), noAZP), false},
		{"sem_token", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := auth.authenticate(bearerRequest(tc.token))
			if tc.ok {
				if err != nil {
					t.Fatalf("authenticate: %v", err)
				}
				if p.ClientID != "provider-a" && p.ClientID != "wallet-service" {
					t.Fatalf("ClientID = %q", p.ClientID)
				}
				return
			}
			if !errors.Is(err, errUnauthenticated) {
				t.Fatalf("err = %v, want errUnauthenticated", err)
			}
		})
	}
}

func TestGuards_ProviderAndInternalAreKeptApart(t *testing.T) {
	key := newTestKey(t)
	auth := newTestAuthenticator(key)
	future := time.Now().Add(time.Hour)

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	provider := auth.Provider(ok)
	internal := auth.Internal(ok)

	providerToken := signWith(t, rs256(key), tokenClaims("provider-a", future))
	internalToken := signWith(t, rs256(key), tokenClaims("wallet-service", future, "wallet-internal"))

	cases := []struct {
		name    string
		handler http.Handler
		token   string
		status  int
		code    string
	}{
		{"provedor_acessa_rota_de_provedor", provider, providerToken, http.StatusNoContent, ""},
		{"interno_nao_acessa_rota_de_provedor", provider, internalToken, http.StatusForbidden, "forbidden"},
		{"interno_acessa_rota_interna", internal, internalToken, http.StatusNoContent, ""},
		{"provedor_nao_acessa_rota_interna", internal, providerToken, http.StatusForbidden, "forbidden"},
		{"sem_token_em_rota_de_provedor", provider, "", http.StatusUnauthorized, "unauthenticated"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.handler.ServeHTTP(rec, bearerRequest(tc.token))

			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if tc.status == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 must carry the WWW-Authenticate challenge")
			}
			if tc.code != "" {
				var body errorBody
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("decode body: %v", err)
				}
				if body.Error.Code != tc.code {
					t.Fatalf("code = %q, want %q", body.Error.Code, tc.code)
				}
			}
		})
	}
}
