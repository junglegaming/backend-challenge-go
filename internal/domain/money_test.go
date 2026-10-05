package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseExternalAmount(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
		want    int64
	}{
		{"valor_valido_duas_casas", "25.00", nil, 2500},
		{"valor_valido_sem_casas", "25", nil, 2500},
		{"valor_valido_uma_casa", "25.5", nil, 2550},
		{"vazio_e_rejeitado", "", ErrInvalidAmount, 0},
		{"nan_e_rejeitado", "NaN", ErrInvalidAmount, 0},
		{"infinity_e_rejeitado", "Infinity", ErrInvalidAmount, 0},
		{"notacao_cientifica_e_rejeitada", "2.5e10", ErrInvalidAmount, 0},
		{"escala_excedente_e_rejeitada", "25.001", ErrInvalidAmount, 0},
		{"negativo_e_rejeitado", "-25.00", ErrInvalidAmount, 0},
		{"overflow_e_rejeitado", "99999999999999999999.00", ErrMoneyOverflow, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseExternalAmount(tt.input, "BRL")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got.AmountMinor() != tt.want {
				t.Fatalf("AmountMinor() = %d, want %d", got.AmountMinor(), tt.want)
			}
		})
	}
}

func TestParseExternalAmount_RejectsInvalidCurrency(t *testing.T) {
	for _, currency := range []string{"", "brl", "BR", "BRLX"} {
		if _, err := ParseExternalAmount("25.00", currency); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("currency %q: err = %v, want ErrInvalidCurrency", currency, err)
		}
	}
}

func TestMoney_AddSub(t *testing.T) {
	a, _ := NewMoney(300, "BRL")
	b, _ := NewMoney(200, "BRL")

	sum, err := a.Add(b)
	if err != nil || sum.AmountMinor() != 500 {
		t.Fatalf("Add = %d, %v, want 500, nil", sum.AmountMinor(), err)
	}

	diff, err := a.Sub(b)
	if err != nil || diff.AmountMinor() != 100 {
		t.Fatalf("Sub = %d, %v, want 100, nil", diff.AmountMinor(), err)
	}
}

func TestMoney_Add_RejectsCurrencyMismatch(t *testing.T) {
	brl, _ := NewMoney(100, "BRL")
	usd, _ := NewMoney(100, "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestMoney_Add_OverflowsAtMaxInt64(t *testing.T) {
	max, _ := NewMoney(math.MaxInt64, "BRL")
	one, _ := NewMoney(1, "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("err = %v, want ErrMoneyOverflow", err)
	}
}

func TestMoney_Negate_OverflowsAtMinInt64(t *testing.T) {
	min, _ := NewMoney(math.MinInt64, "BRL")
	if _, err := min.Negate(); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("err = %v, want ErrMoneyOverflow", err)
	}
}

func TestMoney_String(t *testing.T) {
	tests := []struct {
		minor int64
		want  string
	}{
		{0, "0.00"},
		{1, "0.01"},
		{99, "0.99"},
		{100, "1.00"},
		{9700, "97.00"},
		{-150, "-1.50"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			m, _ := NewMoney(tt.minor, "BRL")
			if got := m.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMoney_JSONRoundTrip(t *testing.T) {
	m, err := ParseExternalAmount("25.00", "BRL")
	if err != nil {
		t.Fatalf("ParseExternalAmount: %v", err)
	}

	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if want := `{"amount":"25.00","currency":"BRL"}`; string(body) != want {
		t.Fatalf("Marshal = %s, want %s", body, want)
	}

	var decoded Money
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !decoded.Equal(m) {
		t.Fatalf("decoded = %v, want %v", decoded, m)
	}
}

func TestMoney_UnmarshalJSON_RejectsInvalidAmount(t *testing.T) {
	var m Money
	err := json.Unmarshal([]byte(`{"amount":"NaN","currency":"BRL"}`), &m)
	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}
}
