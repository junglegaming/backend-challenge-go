//go:build integration

package integration_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

func verifier() *auth.Verifier {
	return auth.NewVerifier(context.Background(), auth.Config{Issuer: env.Issuer(), JWKSURL: env.JWKSURL(), Audience: "wallet-api"})
}

func token(t *testing.T, client string) string {
	t.Helper()
	raw, err := env.Token(context.Background(), client)
	require.NoError(t, err)
	return raw
}

func TestKeycloakIssuesProviderAndInternalIdentities(t *testing.T) {
	t.Parallel()
	v := verifier()
	ctx := context.Background()

	a, err := v.Verify(ctx, token(t, "provider-a"))
	require.NoError(t, err)
	assert.Equal(t, "provider-a", a.ProviderID)
	assert.True(t, a.IsProvider())
	assert.False(t, a.IsInternal())

	b, err := v.Verify(ctx, token(t, "provider-b"))
	require.NoError(t, err)
	assert.Equal(t, "provider-b", b.ProviderID)

	internal, err := v.Verify(ctx, token(t, "wallet-service"))
	require.NoError(t, err)
	assert.True(t, internal.IsInternal())
	assert.Empty(t, internal.ProviderID)

	none, err := v.Verify(ctx, token(t, "no-role-client"))
	require.NoError(t, err)
	assert.False(t, none.IsProvider())
	assert.False(t, none.IsInternal())
}

func TestKeycloakRejectsExpiredAndTamperedTokens(t *testing.T) {
	t.Parallel()
	v := verifier()
	ctx := context.Background()
	shortLived := token(t, "provider-a-short-lived")
	_, err := v.Verify(ctx, shortLived)
	require.NoError(t, err, "valid right after issuance")

	require.Eventually(t, func() bool {
		_, err := v.Verify(ctx, shortLived)
		return errors.Is(err, auth.ErrUnauthenticated)
	}, 10*time.Second, 200*time.Millisecond, "the short-lived token expires")

	raw := token(t, "provider-a")
	tampered := raw[:len(raw)-4] + "AAAA"
	_, err = v.Verify(ctx, tampered)
	require.ErrorIs(t, err, auth.ErrUnauthenticated, "bad signature")

	wrongAudience := auth.NewVerifier(ctx, auth.Config{Issuer: env.Issuer(), JWKSURL: env.JWKSURL(), Audience: "another-api"})
	_, err = wrongAudience.Verify(ctx, raw)
	require.ErrorIs(t, err, auth.ErrUnauthenticated, "audience")
}

// TestProviderIsolationOnReplays re-sends provider-a's operation as
// provider-b, with provider-a's Idempotency-Key and externalTransactionId.
// Both are scoped by provider, so provider-b never gets provider-a's result
// back: naming provider-a in the body is forbidden without any effect, and
// naming itself starts provider-b's own, independent operation.
func TestProviderIsolationOnReplays(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	w := r.http.OpenWallet(t, "100.00")
	ext := uuid.NewString()
	keyA := map[string]string{"Idempotency-Key": "provider-a:" + ext}
	first := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "10.00"), keyA)
	require.Equal(t, http.StatusCreated, first.Status)
	txA := first.Body["transactionId"].(string)

	for _, body := range []string{betBody(w, "provider-a", ext, "10.00"), betBody(w, "provider-a", ext, "20.00")} {
		res := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-b", body, keyA)
		assert.Equal(t, http.StatusForbidden, res.Status)
		assert.Equal(t, "FORBIDDEN", res.Body["code"])
		assert.NotContains(t, res.Body, "transactionId", "no data of provider-a")
		assert.NotContains(t, res.Body, "balance", "no data of provider-a")
	}
	assert.Equal(t, "90.00", r.wallet(t, w.ID)["balance"].(map[string]any)["amount"], "no financial effect")

	own := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-b", betBody(w, "provider-b", ext, "10.00"), keyA)
	require.Equal(t, http.StatusCreated, own.Status, "a new operation of provider-b, not a replay")
	assert.Equal(t, false, own.Body["idempotentReplay"])
	assert.NotEqual(t, txA, own.Body["transactionId"])
	assert.Equal(t, "80.00", own.Body["balance"].(map[string]any)["amount"], "not provider-a's recorded balance")

	byExt := r.http.Call(t, http.MethodGet, "/providers/provider-b/wagering/transactions/"+ext, "provider-b", "", nil)
	require.Equal(t, http.StatusOK, byExt.Status)
	assert.Equal(t, own.Body["transactionId"], byExt.Body["transactionId"])
	assert.Equal(t, http.StatusNotFound, r.http.Call(t, http.MethodGet, "/wagering/transactions/"+txA, "provider-b", "", nil).Status)

	replay := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "10.00"), keyA)
	require.Equal(t, http.StatusOK, replay.Status)
	assert.Equal(t, txA, replay.Body["transactionId"])
	assert.Equal(t, "90.00", replay.Body["balance"].(map[string]any)["amount"], "provider-a's replay is untouched")

	debits, err := testenv.CountDebits(t.Context(), pool, w.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, debits, "one debit per provider, none from the forbidden attempts")
	rec := r.http.Call(t, http.MethodPost, "/wallets/"+w.ID+"/reconciliation", "wallet-service", "", nil)
	assert.Equal(t, true, rec.Body["consistent"])
}
