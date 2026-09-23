package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// maxJWKSBytes bounds the JWKS document read from the IdP.
const maxJWKSBytes = 1 << 20

// keySet verifies JWS signatures against the IdP's JWKS. Keys are fetched
// lazily and cached; a token whose signature matches no cached key triggers
// a refresh at most once per refresh interval, so a flood of forged tokens
// with random key ids cannot turn the API into a JWKS request amplifier.
type keySet struct {
	url          string
	client       *http.Client
	fetchTimeout time.Duration
	interval     time.Duration
	algs         []jose.SignatureAlgorithm

	// mu guards both the immutable cache snapshot and refresh coordination.
	mu          sync.Mutex
	keys        []jose.JSONWebKey
	lastAttempt time.Time
	lastSuccess time.Time
	lastErr     error
	refreshing  bool
	current     *refreshState
}

type refreshState struct {
	done chan struct{}
	keys []jose.JSONWebKey
	err  error
}

// VerifySignature implements oidc.KeySet: it returns the payload of jwt when
// a JWKS key with the token's key id verifies its signature.
func (k *keySet) VerifySignature(ctx context.Context, jwt string) ([]byte, error) {
	jws, err := jose.ParseSigned(jwt, k.algs)
	if err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}
	if len(jws.Signatures) != 1 {
		return nil, errors.New("token must carry exactly one signature")
	}
	kid := jws.Signatures[0].Header.KeyID
	if payload, ok := verify(jws, kid, k.cached()); ok {
		return payload, nil
	}
	keys, err := k.refresh(ctx)
	if err != nil {
		return nil, err
	}
	if payload, ok := verify(jws, kid, keys); ok {
		return payload, nil
	}
	return nil, errors.New("signature does not match any signing key")
}

// verify tries the keys whose id matches kid (any key when kid is empty).
func verify(jws *jose.JSONWebSignature, kid string, keys []jose.JSONWebKey) ([]byte, bool) {
	for i := range keys {
		if kid != "" && keys[i].KeyID != kid {
			continue
		}
		if payload, err := jws.Verify(&keys[i]); err == nil {
			return payload, true
		}
	}
	return nil, false
}

// cached returns the current keys without touching the network.
func (k *keySet) cached() []jose.JSONWebKey {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.keys
}

// refresh joins or starts shared refresh work. The network operation is not
// owned by any caller, so one canceled verification only stops its own wait.
func (k *keySet) refresh(ctx context.Context) ([]jose.JSONWebKey, error) {
	state, start, keys, err := k.prepareRefresh()
	if state == nil {
		return keys, err
	}
	if start {
		k.startRefresh(context.WithoutCancel(ctx), state)
	}
	return k.waitRefresh(ctx, state)
}

// startRefresh receives a cancellation-detached context: shared work must
// outlive any one verifier request and is bounded inside runRefresh.
func (k *keySet) startRefresh(ctx context.Context, state *refreshState) {
	go k.runRefresh(ctx, state)
}

// prepareRefresh returns the current shared attempt, starts a new attempt, or
// returns the cooldown result. lastAttempt throttles failures independently
// from lastSuccess, which records only a cache replacement.
func (k *keySet) prepareRefresh() (*refreshState, bool, []jose.JSONWebKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.refreshing {
		return k.current, false, nil, nil
	}
	if !k.lastAttempt.IsZero() && time.Since(k.lastAttempt) < k.interval {
		return nil, false, k.keys, k.cooldownError()
	}
	state := &refreshState{done: make(chan struct{})}
	k.current = state
	k.refreshing = true
	k.lastAttempt = time.Now()
	return state, true, nil, nil
}

func (k *keySet) cooldownError() error {
	if k.lastErr == nil {
		return nil
	}
	return fmt.Errorf("JWKS refresh cooldown active: %w", k.lastErr)
}

// waitRefresh waits for the caller's prepared attempt or its own
// cancellation, whichever happens first. The explicit state keeps a waiter
// bound to its attempt even when a later refresh replaces k.current.
func (k *keySet) waitRefresh(ctx context.Context, state *refreshState) ([]jose.JSONWebKey, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.done:
		return state.keys, state.err
	}
}

func (k *keySet) runRefresh(base context.Context, state *refreshState) {
	ctx, cancel := context.WithTimeout(base, k.fetchTimeout)
	defer cancel()
	keys, err := k.fetch(ctx)
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrJWKSUnavailable, err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	state.keys = keys
	state.err = err
	k.refreshing = false
	if err == nil {
		k.keys = keys
		k.lastSuccess = time.Now()
		k.lastErr = nil
	} else {
		k.lastErr = err
	}
	close(state.done)
}

// fetch downloads and parses the JWKS document.
func (k *keySet) fetch(ctx context.Context) ([]jose.JSONWebKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.url, nil)
	if err != nil {
		return nil, fmt.Errorf("building JWKS request: %w", err)
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching JWKS: unexpected status %d", resp.StatusCode)
	}
	var set jose.JSONWebKeySet
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxJWKSBytes)).Decode(&set); err != nil {
		return nil, fmt.Errorf("decoding JWKS: %w", err)
	}
	return set.Keys, nil
}
