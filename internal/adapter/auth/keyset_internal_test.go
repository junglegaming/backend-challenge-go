package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type verifyResult struct {
	payload []byte
	err     error
}

func TestKeySetWaiterRemainsBoundToPreparedRefresh(t *testing.T) {
	t.Parallel()
	set := &keySet{}
	first, start, _, err := set.prepareRefresh()
	require.NoError(t, err)
	require.True(t, start)

	set.mu.Lock()
	first.keys = []jose.JSONWebKey{{KeyID: "first"}}
	set.refreshing = false
	set.lastAttempt = time.Time{}
	close(first.done)
	set.mu.Unlock()

	second, start, _, err := set.prepareRefresh()
	require.NoError(t, err)
	require.True(t, start)
	require.NotSame(t, first, second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	keys, err := set.waitRefresh(ctx, first)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "first", keys[0].KeyID)
}

func TestKeySetCallerCancellationDoesNotPoisonSharedRefresh(t *testing.T) {
	t.Parallel()
	key := newSigningKey(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	var hits atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig",
		}}})
	}))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		server.Close()
	})

	set := &keySet{
		url:          server.URL,
		client:       &http.Client{Timeout: time.Second},
		fetchTimeout: time.Second,
		interval:     time.Hour,
		algs:         []jose.SignatureAlgorithm{jose.RS256},
	}
	raw := signedJWS(t, key, "k1")
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan verifyResult, 1)
	go func() {
		payload, err := set.VerifySignature(firstCtx, raw)
		first <- verifyResult{payload: payload, err: err}
	}()

	waitSignal(t, started)
	secondStarted := make(chan struct{})
	second := make(chan verifyResult, 1)
	go func() {
		close(secondStarted)
		payload, err := set.VerifySignature(context.Background(), raw)
		second <- verifyResult{payload: payload, err: err}
	}()
	waitSignal(t, secondStarted)
	cancelFirst()

	firstResult := awaitVerify(t, first)
	assert.ErrorIs(t, firstResult.err, context.Canceled)
	releaseOnce.Do(func() { close(release) })
	secondResult := awaitVerify(t, second)
	require.NoError(t, secondResult.err)
	assert.JSONEq(t, `{"sub":"x"}`, string(secondResult.payload))
	assert.EqualValues(t, 1, hits.Load(), "both callers must share one IdP request")
}

func TestKeySetFailedRefreshRetainsCachedKeys(t *testing.T) {
	t.Parallel()
	key := newSigningKey(t)
	unknownKey := newSigningKey(t)
	var fail atomic.Bool
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig",
		}}})
	}))
	t.Cleanup(server.Close)

	const interval = 10 * time.Millisecond
	set := &keySet{
		url:          server.URL,
		client:       &http.Client{Timeout: time.Second},
		fetchTimeout: time.Second,
		interval:     interval,
		algs:         []jose.SignatureAlgorithm{jose.RS256},
	}
	known := signedJWS(t, key, "k1")
	unknown := signedJWS(t, unknownKey, "k2")
	_, err := set.VerifySignature(context.Background(), known)
	require.NoError(t, err)

	fail.Store(true)
	time.Sleep(2 * interval)
	_, err = set.VerifySignature(context.Background(), unknown)
	assert.ErrorIs(t, err, ErrJWKSUnavailable)
	set.mu.Lock()
	set.interval = time.Hour
	set.mu.Unlock()

	_, err = set.VerifySignature(context.Background(), known)
	assert.NoError(t, err, "a failed refresh must not replace cached keys")
	_, err = set.VerifySignature(context.Background(), unknown)
	assert.ErrorIs(t, err, ErrJWKSUnavailable, "cooldown reuse must retain the operational classification")
	assert.EqualValues(t, 2, hits.Load(), "a failed refresh must throttle retries during the cooldown")
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		require.FailNow(t, "timed out waiting for test signal")
	}
}

func awaitVerify(t *testing.T, result <-chan verifyResult) verifyResult {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-result:
		return got
	case <-timer.C:
		require.FailNow(t, "timed out waiting for verification")
		return verifyResult{}
	}
}

func newSigningKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func signedJWS(t *testing.T, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithHeader("kid", kid),
	)
	require.NoError(t, err)
	signed, err := signer.Sign([]byte(`{"sub":"x"}`))
	require.NoError(t, err)
	raw, err := signed.CompactSerialize()
	require.NoError(t, err)
	return raw
}

// go-oidc parses the compact token before asking the key set to verify it,
// so these keySet branches are unreachable through Verifier.Verify.
func TestKeySetRejectsUnparsableAndMultiSignedTokens(t *testing.T) {
	t.Parallel()
	k := &keySet{algs: []jose.SignatureAlgorithm{jose.RS256}}
	_, err := k.VerifySignature(context.Background(), "not-a-jws")
	assert.ErrorContains(t, err, "parsing token")

	a, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	b, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signer, err := jose.NewMultiSigner([]jose.SigningKey{{Algorithm: jose.RS256, Key: a}, {Algorithm: jose.RS256, Key: b}}, nil)
	require.NoError(t, err)
	jws, err := signer.Sign([]byte(`{"sub":"x"}`))
	require.NoError(t, err)
	_, err = k.VerifySignature(context.Background(), jws.FullSerialize())
	assert.ErrorContains(t, err, "exactly one signature")
}
