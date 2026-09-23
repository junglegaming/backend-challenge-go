package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// WagerDeps are the collaborators of WagerService.
type WagerDeps struct {
	Deps
	// Policy controls the retries of PENDING_REFERENCE operations.
	Policy PendingPolicy
	// ConflictRetries is how many extra attempts follow the first one when a
	// unit of work loses a race (ErrConflict); 0 disables retries.
	ConflictRetries int
}

// WagerService processes provider operations. HTTP, SQS and the pending
// reference worker share it, and therefore share every guarantee.
type WagerService struct {
	WagerDeps
}

// NewWagerService builds the service.
func NewWagerService(d WagerDeps) *WagerService {
	return &WagerService{WagerDeps: d}
}

// SubmitResult is the outcome returned to the provider.
type SubmitResult struct {
	// Transaction is the committed state of the operation: the new one, or
	// the stored one when Replay is set.
	Transaction *wager.Transaction
	// Replay reports an idempotent repeat: nothing changed and the original
	// result is returned as it was.
	Replay bool
}

// Submit processes an operation in its own SQL transaction, retrying lost
// races. Operations without dependencies are concluded synchronously, with
// no intermediate PENDING commit.
func (s *WagerService) Submit(ctx context.Context, cmd SubmitCommand) (SubmitResult, error) {
	start := s.Clock.Now()
	res, err := inTx(ctx, s, "submit", func(ctx context.Context, r Repositories) (SubmitResult, error) {
		return s.submitInTx(ctx, r, cmd, s.Clock.Now())
	})
	if err == nil {
		s.observe(SourceHTTP, res, start)
	}
	return res, err
}

// observe records metrics for a committed submission.
func (s *WagerService) observe(source string, res SubmitResult, start time.Time) {
	s.Metrics.TransactionResult(source, res.Transaction.Status(), res.Replay)
	s.Metrics.ProcessingDuration(source, s.Clock.Now().Sub(start))
}

func (s *WagerService) onConflict(op string) func() {
	return func() { s.Metrics.ConcurrencyConflict(op) }
}

// submitInTx processes an operation inside the caller's unit of work: HTTP
// runs it in a unit of work of its own, the SQS consumer shares the one that
// holds its inbox record. now is the instant of the whole attempt.
//
// The wallet row is locked first, so every operation of a wallet is
// serialized and the idempotency lookup that follows observes any operation
// committed by a competing instance. A race on the same key through another
// wallet is caught by the unique indexes and surfaces as ErrConflict.
func (s *WagerService) submitInTx(ctx context.Context, r Repositories, cmd SubmitCommand, now time.Time) (SubmitResult, error) {
	w, err := r.Wallets().GetForUpdate(ctx, cmd.WalletID)
	if err != nil {
		return SubmitResult{}, err
	}
	if existing, err := findReplay(ctx, r.Transactions(), cmd); err != nil || existing != nil {
		return SubmitResult{Transaction: existing, Replay: existing != nil}, err
	}
	tx, err := wager.NewExternal(cmd.params(s.IDs.New(), now))
	if err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: tx}, s.settle(ctx, r, w, tx, cmd.CausationID, now, true)
}

// findReplay applies the idempotency rules: same key and same payload hash
// replays the stored result; same key with another payload is a conflict;
// the same external id under another key cannot be applied again.
func findReplay(ctx context.Context, repo TransactionRepository, cmd SubmitCommand) (*wager.Transaction, error) {
	rows, err := repo.FindExisting(ctx, cmd.ProviderID, cmd.IdempotencyKey, cmd.ExternalID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	for _, t := range rows {
		if t.External().IdempotencyKey == cmd.IdempotencyKey {
			return matchPayload(t, cmd)
		}
	}
	return nil, ErrDuplicateExternalID
}

func matchPayload(t *wager.Transaction, cmd SubmitCommand) (*wager.Transaction, error) {
	if t.External().PayloadHash != cmd.PayloadHash {
		return nil, ErrIdempotencyConflict
	}
	return t, nil
}

// Caller is the authenticated principal of a query.
type Caller struct {
	// ProviderID scopes a provider to its own transactions.
	ProviderID string
	// Internal grants visibility over every transaction (operators, tests).
	Internal bool
}

func (c Caller) canSee(t *wager.Transaction) bool {
	return c.Internal || (c.ProviderID != "" && t.External().ProviderID == c.ProviderID)
}

// Get returns a transaction visible to the caller. Transactions of other
// providers are reported as not found so their existence does not leak.
func (s *WagerService) Get(ctx context.Context, caller Caller, id uuid.UUID) (*wager.Transaction, error) {
	t, err := s.Queries.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if !caller.canSee(t) {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// GetByExternal returns a transaction by its provider identity.
func (s *WagerService) GetByExternal(ctx context.Context, caller Caller, providerID, externalID string) (*wager.Transaction, error) {
	if !caller.Internal && caller.ProviderID != providerID {
		return nil, ErrForbidden
	}
	return s.Queries.GetTransactionByExternal(ctx, providerID, externalID)
}
