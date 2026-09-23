package app_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

func validInput() app.SubmitInput {
	return app.SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: "t-1", IdempotencyKey: "provider-a:t-1",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "round-987", GameID: "fortune-chimp", Kind: "BET", Amount: "25.00", Currency: "BRL",
	}
}

func TestNewSubmitCommandNormalizesBeforeHashing(t *testing.T) {
	t.Parallel()
	a, err := app.NewSubmitCommand(validInput())
	require.NoError(t, err)

	in := validInput()
	in.PlayerID, in.WalletID = strings.ToUpper(in.PlayerID), strings.ToUpper(in.WalletID)
	in.IdempotencyKey, in.CorrelationID, in.CausationID = "other-key", "c", "m"
	b, err := app.NewSubmitCommand(in)
	require.NoError(t, err)
	assert.Equal(t, a.PayloadHash, b.PayloadHash, "key and transport metadata are not hashed")

	in.Amount = "25.01"
	c, err := app.NewSubmitCommand(in)
	require.NoError(t, err)
	assert.NotEqual(t, a.PayloadHash, c.PayloadHash)
}

func TestNewSubmitCommandRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	mutations := map[string]func(in *app.SubmitInput){
		"player":     func(in *app.SubmitInput) { in.PlayerID = "nope" },
		"nil wallet": func(in *app.SubmitInput) { in.WalletID = uuid.Nil.String() },
		"opening":    func(in *app.SubmitInput) { in.Kind = "OPENING" },
		"float":      func(in *app.SubmitInput) { in.Amount = "25.0" },
		"negative":   func(in *app.SubmitInput) { in.Amount = "-1.00" },
		"currency":   func(in *app.SubmitInput) { in.Currency = "XXX" },
		"zero bet":   func(in *app.SubmitInput) { in.Amount = "0.00" },
		"no key":     func(in *app.SubmitInput) { in.IdempotencyKey = "" },
		"refund ref": func(in *app.SubmitInput) { in.Kind = "REFUND" },
	}
	for name, mutate := range mutations {
		in := validInput()
		mutate(&in)
		_, err := app.NewSubmitCommand(in)
		assert.ErrorIs(t, err, app.ErrValidation, name)
	}
}

func TestNewOpenWalletCommand(t *testing.T) {
	t.Parallel()
	cmd, err := app.NewOpenWalletCommand(app.OpenWalletInput{PlayerID: uuid.NewString(), Amount: "10.00", Currency: "BRL"})
	require.NoError(t, err)
	assert.Equal(t, "10.00", cmd.InitialBalance.Amount())

	_, err = app.NewOpenWalletCommand(app.OpenWalletInput{PlayerID: "x", Amount: "10.00", Currency: "BRL"})
	require.ErrorIs(t, err, app.ErrValidation)
	_, err = app.NewOpenWalletCommand(app.OpenWalletInput{PlayerID: uuid.NewString(), Amount: "1e3", Currency: "BRL"})
	require.ErrorIs(t, err, app.ErrValidation)
}

func TestSystemDefaults(t *testing.T) {
	t.Parallel()
	assert.NotEqual(t, uuid.Nil, app.UUIDv7{}.New())
	assert.Equal(t, uuid.Version(7), app.UUIDv7{}.New().Version())
	assert.False(t, app.SystemClock{}.Now().IsZero())
}

func consumeMsg(t *testing.T, h *harness, id, ext, amount string) app.InboundMessage {
	t.Helper()
	w := h.openWallet(t, "100.00")
	return app.InboundMessage{Consumer: "c", MessageID: id, Hash: "h-" + amount, Command: h.cmd(t, w, op{ext: ext, kind: "BET", amount: amount})}
}

func TestConsumeMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	msg := consumeMsg(t, h, "m-1", "t-1", "10.00")
	ctx := context.Background()

	first, err := h.wagers.ConsumeMessage(ctx, msg)
	require.NoError(t, err)
	assert.False(t, first.Duplicate)

	again, err := h.wagers.ConsumeMessage(ctx, msg)
	require.NoError(t, err)
	assert.True(t, again.Duplicate)
	assert.Equal(t, 1.0, h.counter(t, "wager_transactions_total", map[string]string{"source": app.SourceSQS}), "duplicates are not observed")

	msg.Hash = "changed"
	_, err = h.wagers.ConsumeMessage(ctx, msg)
	require.ErrorIs(t, err, app.ErrInboxConflict)
}

func TestConsumeMessageSharesIdempotencyWithHTTP(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	msg := consumeMsg(t, h, "m-1", "t-1", "10.00")
	_, err := h.wagers.Submit(context.Background(), msg.Command)
	require.NoError(t, err)

	res, err := h.wagers.ConsumeMessage(context.Background(), msg)
	require.NoError(t, err)
	assert.False(t, res.Duplicate)
	assert.True(t, res.Result.Replay, "same operation over SQS is an idempotent replay")
}

func TestConsumeMessageFailuresRollBack(t *testing.T) {
	t.Parallel()
	for _, failing := range []string{"inbox.register", "wallets.get", "inbox.complete"} {
		h := newHarness(t)
		msg := consumeMsg(t, h, "m-1", "t-1", "10.00")
		h.store.failOn(failing, errBoom)
		_, err := h.wagers.ConsumeMessage(context.Background(), msg)
		require.ErrorIs(t, err, errBoom, failing)
		got, err := h.wallets.Get(context.Background(), msg.Command.WalletID)
		require.NoError(t, err, failing)
		assert.Equal(t, "100.00", got.Balance().Amount(), failing)
		assert.Equal(t, 2, h.store.outboxCount(), failing)

		res, err := h.wagers.ConsumeMessage(context.Background(), msg)
		require.NoError(t, err, failing)
		assert.False(t, res.Duplicate, "the inbox row was rolled back: %s", failing)
	}
}
