package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func mustMoney(t *testing.T, amount, cur string) Money {
	t.Helper()
	m, err := ParseMoney(amount, cur)
	if err != nil {
		t.Fatalf("ParseMoney(%q): %v", amount, err)
	}
	return m
}

func TestParseMoneyNormalizesEquivalentForms(t *testing.T) {
	cases := map[string]string{
		"25":                   "25.00",
		"25.5":                 "25.50",
		"25.00":                "25.00",
		"0":                    "0.00",
		"0.01":                 "0.01",
		"92233720368547758.07": "92233720368547758.07",
	}
	for in, want := range cases {
		if got := mustMoney(t, in, "BRL").Amount(); got != want {
			t.Errorf("%q => %q, want %q", in, got, want)
		}
	}
}

func TestParseMoneyRejectsInvalidInput(t *testing.T) {
	inputs := []string{
		"", " ", " 25.00", "25.00 ", "+25", "-25.00", "-0", ".5", "5.", "25.001", "1e3", "1E3",
		"NaN", "Infinity", "-Infinity", "0x10", "25,00", "1_000", "025", "00.10", "abc",
	}
	for _, in := range inputs {
		if _, err := ParseMoney(in, "BRL"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("%q: err = %v, want ErrInvalidAmount", in, err)
		}
	}
}

func TestParseMoneyOverflow(t *testing.T) {
	for _, in := range []string{"92233720368547758.08", "92233720368547759", "999999999999999999999"} {
		if _, err := ParseMoney(in, "BRL"); !errors.Is(err, ErrAmountOverflow) {
			t.Errorf("%q: err = %v, want ErrAmountOverflow", in, err)
		}
	}
}

func TestParseCurrency(t *testing.T) {
	for _, c := range []string{"BRL", "USD", "EUR"} {
		if _, err := ParseCurrency(c); err != nil {
			t.Errorf("%s: %v", c, err)
		}
	}
	for _, c := range []string{"", "brl", "JPY", "BR", "BRLL", "XXX"} {
		if _, err := ParseMoney("1.00", c); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("%q: err = %v, want ErrInvalidCurrency", c, err)
		}
	}
}

func TestMoneyArithmetic(t *testing.T) {
	a, b := mustMoney(t, "100.00", "BRL"), mustMoney(t, "80.50", "BRL")
	sum, _ := a.Add(b)
	diff, _ := b.Sub(a)
	neg, _ := a.Neg()
	if sum.Amount() != "180.50" || diff.Amount() != "-19.50" || neg.Amount() != "-100.00" {
		t.Fatalf("sum=%s diff=%s neg=%s", sum.Amount(), diff.Amount(), neg.Amount())
	}
	if c, _ := a.Compare(b); c != 1 {
		t.Fatalf("compare = %d", c)
	}
	if a.Amount() != "100.00" {
		t.Fatal("operações não podem mutar o operando")
	}
}

func TestMoneyCurrencyMismatch(t *testing.T) {
	brl, usd := mustMoney(t, "1.00", "BRL"), mustMoney(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Compare: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("Equal entre moedas diferentes")
	}
}

func TestMoneyArithmeticOverflow(t *testing.T) {
	max, _ := MoneyFromUnits(math.MaxInt64, "BRL")
	min, _ := MoneyFromUnits(math.MinInt64, "BRL")
	one, _ := MoneyFromUnits(1, "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("Add: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("Neg: %v", err)
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Errorf("Amount(min) = %s", min.Amount())
	}
}

func TestMoneyUninitialized(t *testing.T) {
	var zero Money
	one := mustMoney(t, "1", "BRL")
	if _, err := zero.Add(one); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Add: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Neg: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("Marshal de Money não inicializado deveria falhar")
	}
	if _, err := MoneyFromUnits(1, ""); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("MoneyFromUnits: %v", err)
	}
}

func TestMoneyJSON(t *testing.T) {
	out, err := json.Marshal(mustMoney(t, "25.5", "BRL"))
	if err != nil || string(out) != `{"amount":"25.50","currency":"BRL"}` {
		t.Fatalf("marshal = %s, %v", out, err)
	}
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":"25","currency":"BRL"}`), &m); err != nil || m.Units() != 2500 {
		t.Fatalf("unmarshal = %+v, %v", m, err)
	}
	for _, in := range []string{`{"amount":25.00,"currency":"BRL"}`, `{"amount":"1e2","currency":"BRL"}`, `{"currency":"BRL"}`} {
		if err := json.Unmarshal([]byte(in), &m); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("%s: %v", in, err)
		}
	}
}
