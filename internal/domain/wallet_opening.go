package domain

import (
	"time"

	"github.com/google/uuid"
)

// InternalProviderID is the provider name stored on operations the system
// creates for itself. HTTP and SQS requests never carry it.
const InternalProviderID = "internal"

// WalletOpening is everything a new wallet produces at once. Operation and
// Entry are nil when the opening balance is zero: a zero opening creates no
// OPENING, no ledger line and no events (README §9).
type WalletOpening struct {
	Wallet    *Wallet
	Operation *WagerTransaction
	Entry     *WalletLedgerEntry
}

// OpenWallet creates a wallet at version 1. A positive balance also produces
// the internal OPENING operation, already PROCESSED, and its credit line.
//
// The credit line is built here instead of with Wallet.Credit, because Credit
// bumps the version and an opening must stay at version 1.
func OpenWallet(playerID uuid.UUID, currency string, initial Money) (WalletOpening, error) {
	wallet, err := NewWallet(playerID, currency, initial)
	if err != nil {
		return WalletOpening{}, err
	}
	if initial.IsZero() {
		return WalletOpening{Wallet: wallet}, nil
	}

	operation, err := newOpeningOperation(wallet.ID(), playerID, initial)
	if err != nil {
		return WalletOpening{}, err
	}

	before, err := zero(currency)
	if err != nil {
		return WalletOpening{}, err
	}
	entry, err := newWalletLedgerEntry(wallet.ID(), operation.ID(), DirectionCredit, initial, before, initial)
	if err != nil {
		return WalletOpening{}, err
	}

	return WalletOpening{Wallet: wallet, Operation: operation, Entry: entry}, nil
}

// newOpeningOperation builds the OPENING directly, not through
// NewWagerTransaction, which rejects OPENING on purpose: that constructor is
// the input edge, and OPENING must never arrive from outside.
// An opening has no idempotency key: it happens once per wallet, and
// the wallet's UNIQUE(player_id, currency) is what enforces that.
func newOpeningOperation(walletID, playerID uuid.UUID, amount Money) (*WagerTransaction, error) {
	now := time.Now().UTC()
	op := &WagerTransaction{
		id:                    uuid.New(),
		providerID:            InternalProviderID,
		externalTransactionID: "opening:" + walletID.String(),
		idempotencyKey:        "",
		requestHash:           "",
		walletID:              walletID,
		playerID:              playerID,
		roundID:               InternalProviderID,
		gameID:                InternalProviderID,
		kind:                  WagerKindOpening,
		amount:                amount,
		state:                 WagerStatePending,
		createdAt:             now,
		updatedAt:             now,
	}
	if err := op.MarkProcessed(amount); err != nil {
		return nil, err
	}
	return op, nil
}
