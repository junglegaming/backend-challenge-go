package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
	"github.com/coreos/go-oidc/v3/oidc"
)

// internalRole is the realm role that marks the internal wallet service.
const internalRole = "wallet-internal"

// principal is the caller of a request, read from a token that already passed
// verification. ClientID is the azp claim: for a provider it is the providerId.
type principal struct {
	ClientID string
	Roles    []string
}

func (p principal) isInternal() bool {
	for _, role := range p.Roles {
		if role == internalRole {
			return true
		}
	}
	return false
}

type principalKey struct{}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// principalFrom returns the caller set by a guard. It reports false when the
// request did not pass through one.
func principalFrom(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(principalKey{}).(principal)
	return p, ok
}

// Authenticator verifies the bearer tokens the IdP issues.
type Authenticator struct {
	verifier *oidc.IDTokenVerifier
}

// NewAuthenticator builds a verifier for tokens from issuer. keys is where the
// public keys come from: a RemoteKeySet in production, a static set in unit
// tests.
//
// SkipClientIDCheck is on because Keycloak puts "account" in aud for
// client_credentials tokens. The caller's identity is the azp claim, read below.
// The algorithm list refuses "none" and HMAC tokens, so a token signed with a
// secret the attacker knows cannot pass.
func NewAuthenticator(issuer string, keys oidc.KeySet) *Authenticator {
	return &Authenticator{
		verifier: oidc.NewVerifier(issuer, keys, &oidc.Config{
			SkipClientIDCheck:    true,
			SupportedSigningAlgs: []string{"RS256"},
		}),
	}
}

// authenticate reads the bearer token and verifies signature, issuer,
// algorithm and expiry. Every failure is errUnauthenticated.
func (a *Authenticator) authenticate(r *http.Request) (principal, error) {
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || raw == "" {
		return principal{}, errUnauthenticated
	}

	token, err := a.verifier.Verify(r.Context(), raw)
	if err != nil {
		return principal{}, fmt.Errorf("%w: %v", errUnauthenticated, err)
	}

	var claims struct {
		AZP         string `json:"azp"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err := token.Claims(&claims); err != nil {
		return principal{}, fmt.Errorf("%w: %v", errUnauthenticated, err)
	}
	if claims.AZP == "" {
		return principal{}, fmt.Errorf("%w: token has no azp", errUnauthenticated)
	}

	return principal{ClientID: claims.AZP, Roles: claims.RealmAccess.Roles}, nil
}

// Provider lets through authenticated provider clients. The internal service
// is refused, so it cannot submit operations as if it were a provider.
func (a *Authenticator) Provider(next http.Handler) http.Handler {
	return a.guard(func(p principal) error {
		if p.isInternal() {
			return errForbidden
		}
		return nil
	}, next)
}

// Internal lets through only the internal service.
func (a *Authenticator) Internal(next http.Handler) http.Handler {
	return a.guard(func(p principal) error {
		if !p.isInternal() {
			return errForbidden
		}
		return nil
	}, next)
}

// guard authenticates, applies the check, and passes the caller on through
// the request context. A missing or invalid token gets 401 with the Bearer
// challenge; a valid token that fails the check gets 403.
func (a *Authenticator) guard(check func(principal) error, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := a.authenticate(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wallet"`)
			writeError(w, r, err)
			return
		}
		// From here on every log line for this request says who is calling.
		who := []slog.Attr{slog.String("clientId", p.ClientID)}
		if !p.isInternal() {
			who = append(who, slog.String("providerId", p.ClientID))
		}
		logctx.Add(r.Context(), who...)

		if err := check(p); err != nil {
			writeError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

// Explicação

// - Authenticator delega a validação criptográfica ao go-oidc. Nosso código só faz duas coisas: lê o azp e a role. Isso reduz o que precisa ser defendido em entrevista: a verificação de assinatura e de exp é de uma biblioteca amplamente auditada.
// - SupportedSigningAlgs: RS256 é a linha de segurança mais importante. Sem ela, um token alg: none ou assinado com HMAC usando a chave pública como segredo poderia passar.
// - Os dois guards compartilham guard. A diferença é só a regra: provedor recusa role interna, serviço interno exige a role. Uma única implementação de 401 e de WWW-Authenticate evita divergência entre as rotas.
// - O principal vai para o contexto da requisição. Os handlers vão ler dali, e não mais de um header que o cliente controla.
