package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type Currency string

var supportedCurrencies = map[Currency]struct{}{
	"BRL": {},
	"USD": {},
	"EUR": {},
}

func ParseCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := supportedCurrencies[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return c, nil
}

func (c Currency) Valid() bool {
	_, ok := supportedCurrencies[c]
	return ok
}

const (
	scale       = 2
	unitsPerOne = 100
)

// Aceita "25", "25.5" e "25.00"; tudo é normalizado para escala 2 antes de qualquer hash.
// Escala maior que 2 é rejeitada em vez de arredondada.
var amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(?:\.([0-9]{1,2}))?$`)

// Money guarda o valor em unidades mínimas (centavos) num int64.
// Limite: ±92.233.720.368.547.758,07 na moeda.
type Money struct {
	units    int64
	currency Currency
}

// ParseMoney valida entradas financeiras externas: não aceita valores negativos.
func ParseMoney(amount, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	m := amountPattern.FindStringSubmatch(amount)
	if m == nil {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	whole, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || whole > math.MaxInt64/unitsPerOne {
		return Money{}, fmt.Errorf("%w: %q", ErrAmountOverflow, amount)
	}
	frac := int64(0)
	if m[2] != "" {
		digits := m[2] + strings.Repeat("0", scale-len(m[2]))
		frac, _ = strconv.ParseInt(digits, 10, 64)
	}
	units := whole * unitsPerOne
	if units > math.MaxInt64-frac {
		return Money{}, fmt.Errorf("%w: %q", ErrAmountOverflow, amount)
	}
	return Money{units: units + frac, currency: cur}, nil
}

// MoneyFromUnits reidrata um valor persistido; aceita negativos para diferenças internas.
func MoneyFromUnits(units int64, currency Currency) (Money, error) {
	if !currency.Valid() {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, currency)
	}
	return Money{units: units, currency: currency}, nil
}

func Zero(currency Currency) (Money, error) {
	return MoneyFromUnits(0, currency)
}

func (m Money) Units() int64        { return m.units }
func (m Money) Currency() Currency  { return m.currency }
func (m Money) IsInitialized() bool { return m.currency.Valid() }
func (m Money) IsZero() bool        { return m.units == 0 }
func (m Money) IsPositive() bool    { return m.units > 0 }
func (m Money) IsNegative() bool    { return m.units < 0 }

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.units > 0 && m.units > math.MaxInt64-o.units) || (o.units < 0 && m.units < math.MinInt64-o.units) {
		return Money{}, ErrAmountOverflow
	}
	return Money{units: m.units + o.units, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	neg, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

func (m Money) Neg() (Money, error) {
	if !m.IsInitialized() {
		return Money{}, ErrUninitialized
	}
	if m.units == math.MinInt64 {
		return Money{}, ErrAmountOverflow
	}
	return Money{units: -m.units, currency: m.currency}, nil
}

// Compare retorna -1, 0 ou 1.
func (m Money) Compare(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.units < o.units:
		return -1, nil
	case m.units > o.units:
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.units == o.units
}

// Amount devolve a string decimal com escala fixa de 2 casas, ex.: "25.00", "-0.50".
func (m Money) Amount() string {
	u := m.units
	sign := ""
	var abs uint64
	if u < 0 {
		sign = "-"
		abs = uint64(-(u + 1)) + 1
	} else {
		abs = uint64(u)
	}
	return fmt.Sprintf("%s%d.%02d", sign, abs/unitsPerOne, abs%unitsPerOne)
}

func (m Money) String() string {
	return m.Amount() + " " + string(m.currency)
}

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsInitialized() {
		return nil, ErrUninitialized
	}
	return json.Marshal(moneyJSON{Amount: m.Amount(), Currency: string(m.currency)})
}

// UnmarshalJSON exige amount como string; números JSON são rejeitados para nunca passarem por float.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	var amount string
	if err := json.Unmarshal(raw.Amount, &amount); err != nil {
		return fmt.Errorf("%w: amount deve ser string decimal", ErrInvalidAmount)
	}
	parsed, err := ParseMoney(amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func (m Money) compatible(o Money) error {
	if !m.IsInitialized() || !o.IsInitialized() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s e %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}
