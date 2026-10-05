package httpapi

import (
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

// moneyDTO is the wire form of an amount: a decimal string and a currency.
// It is a string on purpose: a JSON number would pass through a float.
type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m moneyDTO) toDomain() (domain.Money, error) {
	return domain.ParseExternalAmount(m.Amount, m.Currency)
}

type openWalletRequest struct {
	PlayerID       uuid.UUID `json:"playerId"`
	InitialBalance moneyDTO  `json:"initialBalance"`
}

type walletResponse struct {
	ID       uuid.UUID    `json:"id"`
	PlayerID uuid.UUID    `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int64        `json:"version"`
}

type wagerRequest struct {
	ProviderID                     string           `json:"providerId"`
	ExternalTransactionID          string           `json:"externalTransactionId"`
	PlayerID                       uuid.UUID        `json:"playerId"`
	WalletID                       uuid.UUID        `json:"walletId"`
	RoundID                        string           `json:"roundId"`
	GameID                         string           `json:"gameId"`
	Kind                           domain.WagerKind `json:"kind"`
	Money                          moneyDTO         `json:"money"`
	ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId"`
}

// wagerView is what a client sees about one operation. Balance is absent when
// the operation never observed one.
type wagerView struct {
	TransactionID uuid.UUID          `json:"transactionId"`
	Status        domain.WagerState  `json:"status"`
	FailureCode   domain.FailureCode `json:"failureCode,omitempty"`
	Balance       *domain.Money      `json:"balance,omitempty"`
}

// wagerResponse adds the replay flag. Only the POST answers carry it, because
// only a POST can be a replay.
type wagerResponse struct {
	wagerView
	IdempotentReplay bool `json:"idempotentReplay"`
}

func walletView(w *domain.Wallet) walletResponse {
	return walletResponse{
		ID:       w.ID(),
		PlayerID: w.PlayerID(),
		Balance:  w.Balance(),
		Version:  w.Version(),
	}
}

// Explicação

// - moneyDTO.toDomain passa por ParseExternalAmount, o mesmo parser usado pelo domínio. Valor com mais de duas casas, negativo ou vazio vira ErrInvalidAmount e cai em 400.
// - wagerResponse embute wagerView. Em JSON, campos de struct embutida saem no mesmo nível, então a resposta fica plana, como no README.
// - Balance é ponteiro com omitempty. Uma operação sem saldo observado não mostra o campo, em vez de mostrar zero.
