package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

// WagerBody is the business content of one operation. It is the only input to
// the fingerprint. Transport metadata (the Idempotency-Key header, the SQS
// message id) is deliberately absent, so the same operation hashes the same
// whether it arrives by HTTP or by SQS.
type WagerBody struct {
	ProviderID                     string           `json:"providerId"`
	ExternalTransactionID          string           `json:"externalTransactionId"`
	PlayerID                       uuid.UUID        `json:"playerId"`
	WalletID                       uuid.UUID        `json:"walletId"`
	RoundID                        string           `json:"roundId"`
	GameID                         string           `json:"gameId"`
	Kind                           domain.WagerKind `json:"kind"`
	Money                          domain.Money     `json:"money"`
	ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId"`
}

// Fingerprint returns the lowercase hex SHA-256 of body's canonical JSON.
//
// Canonical means three things. Fields are emitted in struct declaration order,
// which is the same as sorted keys because the order is fixed by the type. There
// is no whitespace. Money goes through its own MarshalJSON, so "25", "25.0" and
// "25.00" all become "25.00". A missing reference and an empty one are both the
// zero value "", so they hash the same.
func Fingerprint(body WagerBody) (string, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// Explicação

// - WagerBody é o conteúdo de negócio e nada mais. Header, id de mensagem e transporte não entram. Essa é a garantia de que HTTP e SQS produzem o mesmo hash para a mesma operação.
// - O hash vem do json.Marshal de uma struct, e a ordem dos campos é fixa pelo tipo. Isso equivale a "chaves ordenadas" sem precisar de um passo extra de ordenação.
// - Money serializa pelo próprio MarshalJSON, então 25, 25.0 e 25.00 viram a mesma string antes do hash. O teste de normalização prova isso.
// - O teste de ordem de chaves usa Unmarshal de dois textos diferentes. Ele mostra que o hash depende do conteúdo e não do texto que chegou.
