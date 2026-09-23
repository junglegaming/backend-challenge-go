//go:build integration || e2e

package testenv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/contract"
)

// Response is a decoded JSON response of the API.
type Response struct {
	Status int
	Body   map[string]any
}

// Wallet identifies a wallet opened through the API.
type Wallet struct {
	ID       string
	PlayerID string
}

// Client drives one running instance of the API over HTTP. Token resolves a
// client id (see Clients) to a bearer token; an empty client id sends no
// Authorization header.
type Client struct {
	Base  string
	Token func(ctx context.Context, client string) (string, error)
}

// requestTimeout bounds one call, token fetch included.
const requestTimeout = 10 * time.Second

// Do sends a request and returns transport errors instead of failing, so it
// is safe from worker goroutines.
func (c Client) Do(ctx context.Context, method, path, client, body string, headers map[string]string) (Response, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := c.request(ctx, method, path, client, body, headers)
	if err != nil {
		return Response{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	out := Response{Status: resp.StatusCode}
	if err := json.NewDecoder(resp.Body).Decode(&out.Body); err != nil && !errors.Is(err, io.EOF) {
		return out, err
	}
	return out, nil
}

func (c Client) request(ctx context.Context, method, path, client, body string, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	if client != "" {
		tok, err := c.Token(ctx, client)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// Call is Do on the test goroutine: transport errors fail the test.
func (c Client) Call(t *testing.T, method, path, client, body string, headers map[string]string) Response {
	t.Helper()
	res, err := c.Do(t.Context(), method, path, client, body, headers)
	require.NoError(t, err)
	return res
}

// OpenWallet opens a wallet for a new player with the given BRL balance.
func (c Client) OpenWallet(t *testing.T, amount string) Wallet {
	t.Helper()
	player := uuid.NewString()
	res := c.Call(t, http.MethodPost, "/wallets", "wallet-service",
		`{"playerId":"`+player+`","initialBalance":{"amount":"`+amount+`","currency":"BRL"}}`, nil)
	require.Equal(t, http.StatusCreated, res.Status, res.Body)
	return Wallet{ID: res.Body["id"].(string), PlayerID: player}
}

// Submit posts a provider operation with its idempotency key; it is safe
// from worker goroutines.
func (c Client) Submit(ctx context.Context, w Wallet, provider, ext, kind, amount, ref string) (Response, error) {
	return c.Do(ctx, http.MethodPost, "/wagering/transactions", provider, OperationBody(w, provider, ext, kind, amount, ref),
		map[string]string{"Idempotency-Key": provider + ":" + ext})
}

// SubmitInput builds the unvalidated input of a provider operation on w.
func SubmitInput(w Wallet, provider, ext, kind, amount, ref string) app.SubmitInput {
	return app.SubmitInput{
		ProviderID: provider, ExternalTransactionID: ext, IdempotencyKey: provider + ":" + ext,
		PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-1", GameID: "game-1",
		Kind: kind, Amount: amount, Currency: "BRL", ReferenceExternalTransactionID: ref,
	}
}

// Operation is the wire shape of in (the inverse of contract.Operation.ToInput).
func Operation(in app.SubmitInput) contract.Operation {
	return contract.Operation{
		ProviderID: in.ProviderID, ExternalTransactionID: in.ExternalTransactionID, PlayerID: in.PlayerID, WalletID: in.WalletID,
		RoundID: in.RoundID, GameID: in.GameID, Kind: in.Kind, Money: contract.Money{Amount: in.Amount, Currency: in.Currency},
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	}
}

// OperationBody is the JSON body of a provider operation on w.
func OperationBody(w Wallet, provider, ext, kind, amount, ref string) string {
	raw, err := json.Marshal(Operation(SubmitInput(w, provider, ext, kind, amount, ref)))
	if err != nil {
		panic(err) // strings only: cannot fail
	}
	return string(raw)
}

// Envelope wraps in as a wager-transactions message.
func Envelope(messageID string, in app.SubmitInput) sqsadapter.Envelope {
	return sqsadapter.Envelope{
		MessageID: messageID, Type: sqsadapter.MessageType, OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: sqsadapter.MessageData{Operation: Operation(in), IdempotencyKey: in.IdempotencyKey},
	}
}

// SendMessage sends env to a FIFO queue, grouped by wallet and deduplicated
// by message id.
func SendMessage(ctx context.Context, api sqsadapter.API, queueURL string, env sqsadapter.Envelope) error {
	body, err := sqsadapter.EncodeMessage(env)
	if err != nil {
		return err
	}
	_, err = api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(env.Data.WalletID), MessageDeduplicationId: aws.String(env.MessageID),
	})
	return err
}

// CountDebits counts the DEBIT ledger entries of a wallet.
func CountDebits(ctx context.Context, pool *pgxpool.Pool, walletID string) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&n)
	return n, err
}

// Parallel runs fn n times at once (every goroutine waits on a start gate)
// and collects the results and joined worker errors in call order.
func Parallel[T any](n int, fn func(i int) (T, error)) ([]T, error) {
	values := make([]T, n)
	errs := make([]error, n)
	var wg, ready sync.WaitGroup
	ready.Add(n)
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			ready.Done()
			<-start
			values[i], errs[i] = fn(i)
		})
	}
	ready.Wait()
	close(start)
	wg.Wait()
	return values, errors.Join(errs...)
}
