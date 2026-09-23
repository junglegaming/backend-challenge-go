package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Ledger page size limits.
const (
	// DefaultLedgerLimit is the page size when the caller gives none.
	DefaultLedgerLimit = 50
	// MaxLedgerLimit is the largest page a caller may request.
	MaxLedgerLimit = 200
)

// WalletDeps are the collaborators of WalletService.
type WalletDeps struct {
	Deps
}

// WalletService implements the internal wallet operations.
type WalletService struct {
	WalletDeps
}

// NewWalletService builds the service.
func NewWalletService(d WalletDeps) *WalletService {
	return &WalletService{WalletDeps: d}
}

// Open creates a wallet. A positive initial balance creates, in the same
// commit, the PROCESSED OPENING transaction, its credit entry and the
// WagerTransactionProcessed and WalletBalanceChanged outbox records.
func (s *WalletService) Open(ctx context.Context, cmd OpenWalletCommand) (*wallet.Wallet, error) {
	now := s.Clock.Now()
	openingTxID := s.IDs.New()
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID: s.IDs.New(), PlayerID: cmd.PlayerID, InitialBalance: cmd.InitialBalance,
		OpeningTxID: openingTxID, OpeningEntryID: s.IDs.New(), Now: now,
	})
	if err != nil {
		return nil, invalid(err)
	}
	err = s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
		if err := r.Wallets().Create(ctx, w); err != nil || entry == nil {
			return err
		}
		return s.persistOpening(ctx, r, w, *entry, cmd.CorrelationID)
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *WalletService) persistOpening(ctx context.Context, r Repositories, w *wallet.Wallet, entry wallet.LedgerEntry, correlation string) error {
	meta := func() event.Meta { return newMeta(s.IDs, correlation, "", entry.CreatedAt()) }
	var tx *wager.Transaction
	return sequence(
		func() (err error) {
			tx, err = wager.NewOpening(wager.OpeningParams{
				ID: entry.TransactionID(), WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: entry.Amount(),
				CorrelationID: correlation, Now: entry.CreatedAt(),
			})
			return err
		},
		func() error { return tx.Process(w.Balance(), uuid.Nil, entry.CreatedAt()) },
		func() error { return r.Transactions().Create(ctx, tx) },
		func() error { return r.Ledger().Append(ctx, entry) },
		func() error {
			return r.Outbox().Append(ctx,
				event.NewWagerTransactionProcessed(meta(), tx),
				event.NewWalletBalanceChanged(meta(), entry, w.Version()))
		},
	)
}

// Get returns a wallet.
func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return s.Queries.GetWallet(ctx, id)
}

// LedgerPage is one page of ledger entries in stable (append) order.
type LedgerPage struct {
	// Entries are the page's entries, oldest first.
	Entries []wallet.LedgerEntry
	// NextCursor requests the following page; empty on the last page.
	NextCursor string
}

// Ledger lists entries after an opaque cursor.
func (s *WalletService) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	limit, err = normalizeLimit(limit)
	if err != nil {
		return LedgerPage{}, err
	}
	if _, err := s.Queries.GetWallet(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	rows, err := s.Queries.ListLedger(ctx, walletID, after, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	return buildPage(rows, limit), nil
}

func buildPage(rows []LedgerRow, limit int) LedgerPage {
	page := LedgerPage{Entries: make([]wallet.LedgerEntry, 0, min(len(rows), limit))}
	for _, row := range rows[:min(len(rows), limit)] {
		page.Entries = append(page.Entries, row.Entry)
	}
	if len(rows) > limit {
		page.NextCursor = encodeCursor(rows[limit-1].Seq)
	}
	return page
}

func normalizeLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultLedgerLimit, nil
	}
	if limit < 1 || limit > MaxLedgerLimit {
		return 0, fmt.Errorf("%w: limit must be between 1 and %d", ErrValidation, MaxLedgerLimit)
	}
	return limit, nil
}

const cursorPrefix = "v1:"

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(seq, 10)))
}

func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, invalid(errInvalidCursor)
	}
	seq, err := strconv.ParseInt(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil || seq < 0 {
		return 0, invalid(errInvalidCursor)
	}
	return seq, nil
}

// Reconciliation compares the stored balance with the ledger.
type Reconciliation struct {
	// WalletID is the reconciled wallet.
	WalletID uuid.UUID
	// Stored is the balance kept on the wallet row.
	Stored money.Money
	// Calculated is the balance rebuilt from the ledger (opening included).
	Calculated money.Money
	// Difference is Stored minus Calculated.
	Difference money.Money
	// Consistent reports a zero Difference.
	Consistent bool
	// CheckedEntries is how many ledger entries were summed.
	CheckedEntries int64
}

// Reconcile rebuilds the balance from the ledger (opening included) in one
// consistent snapshot and reports, without changing anything, whether it
// matches the stored balance. difference = stored - calculated.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	snap, err := s.Queries.Reconcile(ctx, walletID)
	if err != nil {
		return Reconciliation{}, err
	}
	calculated, err := money.FromMinor(snap.NetMinor, snap.Stored.Currency())
	if err != nil {
		return Reconciliation{}, err
	}
	diff, err := snap.Stored.Sub(calculated)
	if err != nil {
		return Reconciliation{}, err
	}
	rec := Reconciliation{WalletID: walletID, Stored: snap.Stored, Calculated: calculated,
		Difference: diff, Consistent: diff.IsZero(), CheckedEntries: snap.Entries}
	if !rec.Consistent {
		s.Metrics.ReconciliationDivergence()
		s.Logger.ErrorContext(ctx, "wallet reconciliation divergence", "walletId", walletID,
			"stored", snap.Stored.Amount(), "calculated", calculated.Amount(), "difference", diff.Amount())
	}
	return rec, nil
}
