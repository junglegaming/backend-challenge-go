package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// PayloadHash calcula SHA-256 do JSON canônico dos campos de negócio.
// encoding/json ordena as chaves de map; UUIDs vão em minúsculas com hífens e o valor
// já normalizado em escala 2. idempotencyKey e metadados de transporte ficam de fora,
// o que torna o hash idêntico para HTTP e SQS.
func PayloadHash(r ExternalRequest) (string, error) {
	fields := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]string{
			"amount":   r.Money.Amount(),
			"currency": string(r.Money.Currency()),
		},
	}
	if r.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	canonical, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
