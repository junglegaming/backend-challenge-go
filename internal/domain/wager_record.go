package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// WagerRecord is the full persisted shape of a WagerTransaction. The adapter
// saves an operation from it and rebuilds one from it, so storage never has
// to re-run the rules that already held when the row was written.
type WagerRecord struct {
	ID                             uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	RequestHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           WagerKind
	Amount                         Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	State                          WagerState
	FailureCode                    FailureCode
	BalanceAfter                   *Money
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// Record returns the persisted shape of w.
func (w *WagerTransaction) Record() WagerRecord {
	return WagerRecord{
		ID:                             w.id,
		ProviderID:                     w.providerID,
		ExternalTransactionID:          w.externalTransactionID,
		IdempotencyKey:                 w.idempotencyKey,
		RequestHash:                    w.requestHash,
		WalletID:                       w.walletID,
		PlayerID:                       w.playerID,
		RoundID:                        w.roundID,
		GameID:                         w.gameID,
		Kind:                           w.kind,
		Amount:                         w.amount,
		ReferenceExternalTransactionID: w.referenceExternalTransactionID,
		ReferenceTransactionID:         w.referenceTransactionID,
		State:                          w.state,
		FailureCode:                    w.failureCode,
		BalanceAfter:                   w.balanceAfter,
		CreatedAt:                      w.createdAt,
		UpdatedAt:                      w.updatedAt,
	}
}

// RehydrateWagerTransaction rebuilds an operation from storage. Like
// RehydrateWallet, it trusts the row: no validation, no transition, no event.
func RehydrateWagerTransaction(rec WagerRecord) *WagerTransaction {
	return &WagerTransaction{
		id:                             rec.ID,
		providerID:                     rec.ProviderID,
		externalTransactionID:          rec.ExternalTransactionID,
		idempotencyKey:                 rec.IdempotencyKey,
		requestHash:                    rec.RequestHash,
		walletID:                       rec.WalletID,
		playerID:                       rec.PlayerID,
		roundID:                        rec.RoundID,
		gameID:                         rec.GameID,
		kind:                           rec.Kind,
		amount:                         rec.Amount,
		referenceExternalTransactionID: rec.ReferenceExternalTransactionID,
		referenceTransactionID:         rec.ReferenceTransactionID,
		state:                          rec.State,
		failureCode:                    rec.FailureCode,
		balanceAfter:                   rec.BalanceAfter,
		createdAt:                      rec.CreatedAt,
		updatedAt:                      rec.UpdatedAt,
	}
}

// BindReference records which internal operation this one reverses. It is
// allowed only while the operation is PENDING, before its effect is applied,
// so a reversal always points at the operation it actually undoes.
func (w *WagerTransaction) BindReference(referenceID uuid.UUID) error {
	if w.state != WagerStatePending {
		return fmt.Errorf("%w: cannot bind a reference from %s", ErrInvalidWagerTransition, w.state)
	}
	if referenceID == uuid.Nil {
		return fmt.Errorf("%w: reference id is required", ErrInvalidWagerTransition)
	}
	w.referenceTransactionID = referenceID
	w.touch()
	return nil
}
