package app

import (
	"encoding/json"
	"testing"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

const (
	testPlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	testWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
)

func betBody(t *testing.T, amount string) WagerBody {
	t.Helper()
	money, err := domain.ParseExternalAmount(amount, "BRL")
	if err != nil {
		t.Fatalf("ParseExternalAmount(%q): %v", amount, err)
	}
	return WagerBody{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              mustUUID(t, testPlayerID),
		WalletID:              mustUUID(t, testWalletID),
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  domain.WagerKindBet,
		Money:                 money,
	}
}

func mustFingerprint(t *testing.T, body WagerBody) string {
	t.Helper()
	h, err := Fingerprint(body)
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	return h
}

func mustUUID(t *testing.T, raw string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(raw)
	if err != nil {
		t.Fatalf("uuid.Parse(%q): %v", raw, err)
	}
	return id
}

func TestFingerprint_SameBodySameHash(t *testing.T) {
	if mustFingerprint(t, betBody(t, "25.00")) != mustFingerprint(t, betBody(t, "25.00")) {
		t.Fatal("the same body must hash the same")
	}
}

func TestFingerprint_AmountIsNormalized(t *testing.T) {
	base := mustFingerprint(t, betBody(t, "25.00"))
	for _, raw := range []string{"25", "25.0"} {
		if got := mustFingerprint(t, betBody(t, raw)); got != base {
			t.Fatalf("amount %q hashed differently from 25.00", raw)
		}
	}
}

func TestFingerprint_KeyOrderAndWhitespaceDoNotMatter(t *testing.T) {
	compact := `{"providerId":"provider-a","externalTransactionId":"transaction-123",` +
		`"playerId":"` + testPlayerID + `","walletId":"` + testWalletID + `",` +
		`"roundId":"round-987","gameId":"fortune-chimp","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},"referenceExternalTransactionId":""}`
	reordered := `{
              "money": {"currency": "BRL", "amount": "25.00"},
              "kind": "BET", "gameId": "fortune-chimp", "roundId": "round-987",
              "walletId": "` + testWalletID + `", "playerId": "` + testPlayerID + `",
              "externalTransactionId": "transaction-123", "providerId": "provider-a"
      }`

	var a, b WagerBody
	if err := json.Unmarshal([]byte(compact), &a); err != nil {
		t.Fatalf("unmarshal compact: %v", err)
	}
	if err := json.Unmarshal([]byte(reordered), &b); err != nil {
		t.Fatalf("unmarshal reordered: %v", err)
	}
	if mustFingerprint(t, a) != mustFingerprint(t, b) {
		t.Fatal("key order and whitespace changed the hash")
	}
}

func TestFingerprint_BusinessFieldChangesHash(t *testing.T) {
	base := mustFingerprint(t, betBody(t, "25.00"))

	changes := map[string]func(*WagerBody){
		"amount": func(b *WagerBody) {
			b.Money, _ = domain.ParseExternalAmount("25.01", "BRL")
		},
		"kind": func(b *WagerBody) {
			b.Kind = domain.WagerKindWin
		},
		"round": func(b *WagerBody) {
			b.RoundID = "round-988"
		},
		"reference": func(b *WagerBody) {
			b.ReferenceExternalTransactionID = "transaction-100"
		},
	}

	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			body := betBody(t, "25.00")
			change(&body)
			if mustFingerprint(t, body) == base {
				t.Fatalf("changing %s did not change the hash", name)
			}
		})
	}
}
