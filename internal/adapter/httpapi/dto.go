package httpapi

import (
	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/contract"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// optionalMoney renders a domain amount, or nothing when it is unset.
func optionalMoney(m money.Money) *contract.Money {
	if m.Validate() != nil {
		return nil
	}
	v := contract.NewMoney(m)
	return &v
}

// openWalletRequest is the body of POST /wallets.
type openWalletRequest struct {
	PlayerID       string         `json:"playerId"`
	InitialBalance contract.Money `json:"initialBalance"`
}

// walletResponse is the wallet resource.
type walletResponse struct {
	ID        string          `json:"id"`
	PlayerID  string          `json:"playerId"`
	Balance   *contract.Money `json:"balance"`
	Version   int64           `json:"version"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
}

// newWalletResponse renders a wallet.
func newWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID().String(), PlayerID: w.PlayerID().String(), Balance: optionalMoney(w.Balance()),
		Version: w.Version(), CreatedAt: contract.FormatTime(w.CreatedAt()), UpdatedAt: contract.FormatTime(w.UpdatedAt()),
	}
}

// ledgerEntryResponse is one ledger line.
type ledgerEntryResponse struct {
	ID            string          `json:"id"`
	TransactionID string          `json:"transactionId"`
	Direction     string          `json:"direction"`
	Money         *contract.Money `json:"money"`
	BalanceBefore *contract.Money `json:"balanceBefore"`
	BalanceAfter  *contract.Money `json:"balanceAfter"`
	CreatedAt     string          `json:"createdAt"`
}

// ledgerResponse is a page of the wallet ledger.
type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Items      []ledgerEntryResponse `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

// newLedgerResponse renders a ledger page.
func newLedgerResponse(walletID uuid.UUID, page app.LedgerPage) ledgerResponse {
	items := make([]ledgerEntryResponse, 0, len(page.Entries))
	for _, e := range page.Entries {
		items = append(items, ledgerEntryResponse{
			ID: e.ID().String(), TransactionID: e.TransactionID().String(), Direction: string(e.Direction()),
			Money: optionalMoney(e.Amount()), BalanceBefore: optionalMoney(e.BalanceBefore()),
			BalanceAfter: optionalMoney(e.BalanceAfter()), CreatedAt: contract.FormatTime(e.CreatedAt()),
		})
	}
	return ledgerResponse{WalletID: walletID.String(), Items: items, NextCursor: page.NextCursor}
}

// reconciliationResponse compares the stored balance with the ledger.
type reconciliationResponse struct {
	WalletID          string          `json:"walletId"`
	StoredBalance     *contract.Money `json:"storedBalance"`
	CalculatedBalance *contract.Money `json:"calculatedBalance"`
	Difference        *contract.Money `json:"difference"`
	Consistent        bool            `json:"consistent"`
	CheckedEntries    int64           `json:"checkedEntries"`
}

// newReconciliationResponse renders a reconciliation.
func newReconciliationResponse(r app.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID: r.WalletID.String(), StoredBalance: optionalMoney(r.Stored), CalculatedBalance: optionalMoney(r.Calculated),
		Difference: optionalMoney(r.Difference), Consistent: r.Consistent, CheckedEntries: r.CheckedEntries,
	}
}

// submitRequest is the body of POST /wagering/transactions: the shared
// operation shape, with the idempotency key carried in the header.
type submitRequest struct {
	contract.Operation
}

// outcomeDTO is the processing result shared by submit and read responses.
type outcomeDTO struct {
	Status        string          `json:"status"`
	FailureCode   string          `json:"failureCode,omitempty"`
	Balance       *contract.Money `json:"balance,omitempty"`
	NextAttemptAt string          `json:"nextAttemptAt,omitempty"`
}

// newOutcomeDTO renders the state of a transaction.
func newOutcomeDTO(t *wager.Transaction) outcomeDTO {
	return outcomeDTO{
		Status: string(t.Status()), FailureCode: string(t.FailureCode()),
		Balance: optionalMoney(t.ResultBalance()), NextAttemptAt: optionalTime(t),
	}
}

// submitResponse is the body of POST /wagering/transactions.
type submitResponse struct {
	TransactionID string `json:"transactionId"`
	outcomeDTO
	IdempotentReplay bool `json:"idempotentReplay"`
}

// newSubmitResponse renders a submit result.
func newSubmitResponse(res app.SubmitResult) submitResponse {
	t := res.Transaction
	return submitResponse{TransactionID: t.ID().String(), outcomeDTO: newOutcomeDTO(t), IdempotentReplay: res.Replay}
}

// optionalTime renders the next attempt of a pending operation.
func optionalTime(t *wager.Transaction) string {
	if t.Status() != wager.StatusPendingReference {
		return ""
	}
	return contract.FormatTime(t.NextAttemptAt())
}

// transactionResponse is the transaction resource.
type transactionResponse struct {
	TransactionID string `json:"transactionId"`
	Origin        string `json:"origin"`
	Kind          string `json:"kind"`
	outcomeDTO
	WalletID                       string          `json:"walletId"`
	PlayerID                       string          `json:"playerId"`
	Money                          *contract.Money `json:"money"`
	ProviderID                     string          `json:"providerId,omitempty"`
	ExternalTransactionID          string          `json:"externalTransactionId,omitempty"`
	RoundID                        string          `json:"roundId,omitempty"`
	GameID                         string          `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string          `json:"referenceTransactionId,omitempty"`
	Attempts                       int             `json:"attempts"`
	CreatedAt                      string          `json:"createdAt"`
	UpdatedAt                      string          `json:"updatedAt"`
}

// newTransactionResponse renders a transaction.
func newTransactionResponse(t *wager.Transaction) transactionResponse {
	ext := t.External()
	resp := transactionResponse{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()), outcomeDTO: newOutcomeDTO(t),
		WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(), Money: optionalMoney(t.Amount()),
		ProviderID: ext.ProviderID, ExternalTransactionID: ext.ExternalID, RoundID: ext.RoundID, GameID: ext.GameID,
		ReferenceExternalTransactionID: ext.ReferenceExternalID, Attempts: t.Attempts(),
		CreatedAt: contract.FormatTime(t.CreatedAt()), UpdatedAt: contract.FormatTime(t.UpdatedAt()),
	}
	if t.ReferenceTxID() != uuid.Nil {
		resp.ReferenceTransactionID = t.ReferenceTxID().String()
	}
	return resp
}

// errorResponse is the body of every error.
type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
