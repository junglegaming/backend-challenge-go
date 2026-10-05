package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

const (
	keyStateInFlight  = "in_flight"
	keyStateCompleted = "completed"

	statusProcessed = 201
	statusRejected  = 422
)

// IdempotencyKey identifies a request. It is scoped by the owner of the key,
// the endpoint and the key itself, so two owners can reuse the same string.
type IdempotencyKey struct {
	Scope    string
	Endpoint string
	Key      string
}

// WagerRequest is one operation to process. RequestHash is the hash of the
// canonical body, computed by the caller so HTTP and SQS produce the same one.
type WagerRequest struct {
	Key         IdempotencyKey
	RequestHash string
	Command     domain.WagerCommand
	Inbox       *InboxMessage
}

// Outcome is the answer for a request. A first request and every replay of
// it receive the same StatusCode and Body; only Replayed tells them apart.
type Outcome struct {
	TransactionID uuid.UUID
	StatusCode    int
	Body          []byte
	Replayed      bool
}

// WagerProcessor applies operations atomically with their idempotency record.
type WagerProcessor struct {
	pool    *Pool
	wallets *WalletRepository
}

// NewWagerProcessor builds a WagerProcessor backed by pool.
func NewWagerProcessor(pool *Pool) *WagerProcessor {
	return &WagerProcessor{pool: pool, wallets: NewWalletRepository(pool)}
}

// Process runs one operation inside one transaction. The inbox row (when the
// request is a message) and the idempotency key are claimed first. A first
// attempt executes; a repeat gets the stored outcome. Everything commits
// together, or nothing does.
func (p *WagerProcessor) Process(ctx context.Context, req WagerRequest) (Outcome, error) {
	ctx, cancel := p.pool.withAcquireTimeout(ctx)
	defer cancel()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("postgres: begin tx: %w", err)
	}
	// After a successful Commit this is a no-op. On any error it undoes the
	// inbox row and the key claim, so a failed attempt leaves nothing behind.
	defer func() { _ = tx.Rollback(ctx) }()

	if req.Inbox != nil {
		if err := claimInbox(ctx, tx, *req.Inbox); err != nil {
			return Outcome{}, err
		}
	}

	claimed, err := claimKey(ctx, tx, req)
	if err != nil {
		return Outcome{}, err
	}

	var outcome Outcome
	if claimed {
		outcome, err = p.execute(ctx, tx, req)
	} else {
		outcome, err = replayKey(ctx, tx, req)
	}
	if err != nil {
		return Outcome{}, err
	}

	// A replay also completes the inbox row. Otherwise a message whose operation
	// was processed through HTTP would never be marked as done here.
	if req.Inbox != nil {
		if err := completeInbox(ctx, tx, *req.Inbox); err != nil {
			return Outcome{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, fmt.Errorf("postgres: commit tx: %w", err)
	}
	return outcome, nil
}

// claimKey inserts the key as in_flight. The primary key decides the race: a
// concurrent request with the same key blocks on this insert until the first
// transaction ends, then reports zero rows inserted.
func claimKey(ctx context.Context, tx pgx.Tx, req WagerRequest) (bool, error) {
	const query = `INSERT INTO idempotency_keys (scope, endpoint, key, request_hash, state)
		VALUES ($1, $2, $3, $4, 'in_flight')
		ON CONFLICT DO NOTHING`

	tag, err := tx.Exec(ctx, query, req.Key.Scope, req.Key.Endpoint, req.Key.Key, req.RequestHash)
	if err != nil {
		return false, fmt.Errorf("postgres: claim idempotency key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// replayKey answers a key that was already claimed. A different body is a
// conflict; a key still in flight is a conflict too; a completed key returns
// the stored outcome, unchanged.
func replayKey(ctx context.Context, tx pgx.Tx, req WagerRequest) (Outcome, error) {
	const query = `SELECT request_hash, state, COALESCE(status_code, 0),
		COALESCE(response_body, ''), wager_transaction_id
		FROM idempotency_keys
		WHERE scope = $1 AND endpoint = $2 AND key = $3`

	var (
		hash, state string
		status      int
		body        string
		txID        *uuid.UUID
	)
	err := tx.QueryRow(ctx, query, req.Key.Scope, req.Key.Endpoint, req.Key.Key).
		Scan(&hash, &state, &status, &body, &txID)
	if err != nil {
		return Outcome{}, fmt.Errorf("postgres: load idempotency key: %w", err)
	}

	if hash != req.RequestHash {
		return Outcome{}, ErrIdempotencyKeyReuse
	}
	if state == keyStateInFlight {
		return Outcome{}, ErrRequestInFlight
	}

	out := Outcome{StatusCode: status, Body: []byte(body), Replayed: true}
	if txID != nil {
		out.TransactionID = *txID
	}
	return out, nil
}

// settle decides what the operation does to the locked wallet and records the
// decision on wt. It returns the ledger entry to persist, or nil when the
// wallet does not change (LOSS, or a business rejection).
func (p *WagerProcessor) settle(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction, wallet *domain.Wallet) (*domain.WalletLedgerEntry, error) {
	switch wt.Kind() {
	case domain.WagerKindLoss:
		// A LOSS moves no money, so it produces no ledger entry and does not
		// bump the wallet version. It only records what the balance was. No
		// movement means Debit and Credit never check the currency, so check it here.
		if wt.Amount().Currency() != wallet.Currency() {
			return nil, domain.ErrCurrencyMismatch
		}
		return nil, wt.MarkProcessed(wallet.Balance())

	case domain.WagerKindBet:
		return apply(wt, wallet, wallet.Debit, domain.FailureInsufficientFunds)

	case domain.WagerKindWin:
		// A credit cannot fail for lack of funds, so no failure code is needed.
		return apply(wt, wallet, wallet.Credit, "")

	case domain.WagerKindRefund, domain.WagerKindRollback:
		return p.reverse(ctx, tx, wt, wallet)

	default:
		return nil, fmt.Errorf("postgres: no settlement rule for kind %q", wt.Kind())
	}
}

// reverse undoes the operation a REFUND or ROLLBACK references, after the
// domain has checked the reference. Undoing a BET gives money back; undoing a
// WIN or a REFUND takes it away again.
func (p *WagerProcessor) reverse(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction, wallet *domain.Wallet) (*domain.WalletLedgerEntry, error) {
	referenced, err := findWagerByExternalID(ctx, tx, wt.ProviderID(), wt.ReferenceExternalTransactionID())
	if err != nil {
		return nil, err
	}

	already := false
	if referenced != nil {
		already, err = hasSuccessfulReversal(ctx, tx, referenced.ID())
		if err != nil {
			return nil, err
		}
	}

	if code := domain.ValidateReversal(wt, referenced, already); code != "" {
		return nil, wt.MarkRejected(code)
	}
	if err := wt.BindReference(referenced.ID()); err != nil {
		return nil, err
	}

	if referenced.Kind() == domain.WagerKindBet {
		return apply(wt, wallet, wallet.Credit, "")
	}
	return apply(wt, wallet, wallet.Debit, domain.FailureReversalInsufficientFunds)
}

// apply performs one movement against the wallet. An insufficient-funds
// result becomes a recorded business rejection, not an error: the decision
// must be stored and replayed. Any other failure is returned unchanged.
func apply(
	wt *domain.WagerTransaction,
	wallet *domain.Wallet,
	movement func(uuid.UUID, domain.Money) (*domain.WalletLedgerEntry, error),
	insufficientCode domain.FailureCode,
) (*domain.WalletLedgerEntry, error) {
	entry, err := movement(wt.ID(), wt.Amount())
	if errors.Is(err, domain.ErrInsufficientFunds) {
		return nil, wt.MarkRejected(insufficientCode)
	}
	if err != nil {
		return nil, err
	}
	return entry, wt.MarkProcessed(wallet.Balance())
}

// wagerResponse is the body stored with the key and returned to the provider.
type wagerResponse struct {
	TransactionID uuid.UUID          `json:"transactionId"`
	Status        domain.WagerState  `json:"status"`
	FailureCode   domain.FailureCode `json:"failureCode,omitempty"`
	Balance       domain.Money       `json:"balance"`
}

// newOutcome builds the stored answer: 201 when the operation was processed,
// 422 when the business rules rejected it. The body carries the balance the
// wallet had when the decision was made, so a replay can report it later.
func newOutcome(wt *domain.WagerTransaction, wallet *domain.Wallet) (Outcome, error) {
	status := statusRejected
	if wt.State() == domain.WagerStateProcessed {
		status = statusProcessed
	}

	body, err := json.Marshal(wagerResponse{
		TransactionID: wt.ID(),
		Status:        wt.State(),
		FailureCode:   wt.FailureCode(),
		Balance:       wallet.Balance(),
	})
	if err != nil {
		return Outcome{}, fmt.Errorf("postgres: encode response: %w", err)
	}
	return Outcome{TransactionID: wt.ID(), StatusCode: status, Body: body}, nil
}

func completeKey(ctx context.Context, tx pgx.Tx, key IdempotencyKey, out Outcome) error {
	const query = `UPDATE idempotency_keys
		SET state = 'completed', status_code = $4, response_body = $5, wager_transaction_id = $6
		WHERE scope = $1 AND endpoint = $2 AND key = $3`

	tag, err := tx.Exec(ctx, query,
		key.Scope, key.Endpoint, key.Key, out.StatusCode, string(out.Body), out.TransactionID)
	if err != nil {
		return fmt.Errorf("postgres: complete idempotency key: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("postgres: complete idempotency key: %d rows updated, want 1", tag.RowsAffected())
	}
	return nil
}

// execute is the work of a first attempt. It runs only for the request that
// claimed the key: decide the operation, move the money, store the operation
// and the response.
func (p *WagerProcessor) execute(ctx context.Context, tx pgx.Tx, req WagerRequest) (Outcome, error) {
	wt, err := domain.NewWagerTransaction(req.Command)
	if err != nil {
		return Outcome{}, err
	}

	wallet, err := p.wallets.LockForUpdate(ctx, tx, wt.WalletID())
	if err != nil {
		return Outcome{}, err
	}
	// The wallet is locked, so its owner cannot change under us. An operation
	// that names another player is refused before any money moves, and the
	// error rolls the whole transaction back, key claim included.
	if wallet.PlayerID() != wt.PlayerID() {
		return Outcome{}, ErrWalletOwnerMismatch
	}
	previousVersion := wallet.Version()

	entry, err := p.settle(ctx, tx, wt, wallet)
	if err != nil {
		return Outcome{}, err
	}
	if entry != nil {
		if err := p.wallets.Save(ctx, tx, wallet, previousVersion); err != nil {
			return Outcome{}, err
		}
		if err := p.wallets.InsertLedgerEntry(ctx, tx, entry); err != nil {
			return Outcome{}, err
		}
	}

	// The events commit with the operation. A replay never reaches this point, so
	// it adds none, and any error below rolls them back with the rest.
	//
	// They are written before the operation row on purpose: inserting that row is
	// where a duplicate (provider, externalTransactionId) is refused, so a refusal
	// there has to undo events that were already written. A test relies on it.
	events := domain.EventsFor(wt, entry, wallet.Version(), logctx.CorrelationID(ctx))
	if err := insertEvents(ctx, tx, events); err != nil {
		return Outcome{}, err
	}

	if err := insertWagerTransaction(ctx, tx, wt); err != nil {
		return Outcome{}, err
	}

	outcome, err := newOutcome(wt, wallet)
	if err != nil {
		return Outcome{}, err
	}
	if err := completeKey(ctx, tx, req.Key, outcome); err != nil {
		return Outcome{}, err
	}
	return outcome, nil
}

// Explicação

// - A ordem das reivindicações é: inbox, depois chave de idempotência, depois a carteira com FOR UPDATE. Todas dentro da mesma transação.
// - Antes, a mudança era só a criação do inbox. Mas havia um caminho em que o replay saía sem commit. Como a linha do inbox também fica nessa transação, ela seria desfeita. Por isso Process agora sempre chega ao Commit, seja primeira execução ou replay.
// - execute é o mesmo corpo que estava dentro de Process, só separado. Os testes existentes do processador continuam valendo porque a regra não mudou.
