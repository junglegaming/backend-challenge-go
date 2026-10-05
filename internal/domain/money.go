package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// externalAmountPattern accepts only digits, an optional single '.', and at
// most two digits after it. This single pattern is what rejects empty
// strings, "NaN", "Infinity", scientific notation, a leading sign, and more
// than two decimal places — all in one place, instead of five separate checks.
var externalAmountPattern = regexp.MustCompile(`^(\d+)(?:\.(\d{1,2}))?$`)

// Money is an immutable value object: an amount in integer minor units
// (cents, for a 2-decimal currency) paired with its ISO 4217 code. There is
// no float anywhere in its representation, construction, or arithmetic.
type Money struct {
	amountMinor int64
	currency    string
}

// NewMoney builds a Money directly from minor units. It is the permissive,
// internal constructor: negative values are allowed (a Sub result, a diff),
// the only thing validated is that currency looks like a real code. Use
// ParseExternalAmount instead at any HTTP/SQS boundary.
func NewMoney(amountMinor int64, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}
	return Money{amountMinor: amountMinor, currency: currency}, nil
}

// Zero returns the zero amount for currency.
func zero(currency string) (Money, error) {
	return NewMoney(0, currency)
}

// ParseExternalAmount parses a decimal string received from outside the
// system (an HTTP body, an SQS message) into Money. It is strict on
// purpose: structurally invalid input is rejected before any arithmetic
// happens, and a negative amount is always rejected here — external
// financial inputs are never allowed to be negative, even though internal
// calculations (NewMoney, Sub) may produce a negative Money.
func ParseExternalAmount(raw, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Money{}, ErrInvalidAmount
	}

	matches := externalAmountPattern.FindStringSubmatch(raw)
	if matches == nil {
		return Money{}, ErrInvalidAmount
	}

	minor, err := minorFromParts(matches[1], matches[2])
	if err != nil {
		return Money{}, err
	}
	return Money{amountMinor: minor, currency: currency}, nil
}

// minorFromParts converts the integer and fractional parts matched by
// externalAmountPattern into minor units, checking overflow before it
// happens rather than after. intPart has no practical length limit from the
// regex, so it alone can overflow int64 even before the *100 scaling.
func minorFromParts(intPart, fracPart string) (int64, error) {
	whole, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, ErrMoneyOverflow
	}

	for len(fracPart) < 2 {
		fracPart += "0"
	}
	fraction, err := strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return 0, ErrInvalidAmount
	}

	const maxWhole = math.MaxInt64 / 100
	if whole > maxWhole {
		return 0, ErrMoneyOverflow
	}
	return whole*100 + fraction, nil
}

func validateCurrency(code string) error {
	if len(code) != 3 {
		return ErrInvalidCurrency
	}
	for _, r := range code {
		if r < 'A' || r > 'Z' {
			return ErrInvalidCurrency
		}
	}
	return nil
}

// AmountMinor returns the raw minor-unit value (cents, for BRL).
func (m Money) AmountMinor() int64 { return m.amountMinor }

// Currency returns the ISO 4217 code.
func (m Money) Currency() string { return m.currency }

func (m Money) IsPositive() bool { return m.amountMinor > 0 }
func (m Money) IsNegative() bool { return m.amountMinor < 0 }
func (m Money) IsZero() bool     { return m.amountMinor == 0 }

// Equal compares both amount and currency. Two Monies of different
// currencies are never equal, even if the minor-unit value matches.
func (m Money) Equal(other Money) bool {
	return m.currency == other.currency && m.amountMinor == other.amountMinor
}

// Add returns m + other. Both must share a currency.
// Overflow detection for signed addition without risking an
// intermediate overflow of its own: if adding a positive b made the
// sum smaller, or adding a negative b made it larger, it wrapped.
func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	a, b := m.amountMinor, other.amountMinor
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amountMinor: sum, currency: m.currency}, nil
}

// Sub returns m - other. Both must share a currency.
func (m Money) Sub(other Money) (Money, error) {
	negated, err := other.Negate()
	if err != nil {
		return Money{}, err
	}
	return m.Add(negated)
}

// Negate returns -m.
// -math.MinInt64 does not fit in an int64; there is one more
// negative value than there are positive ones.
func (m Money) Negate() (Money, error) {
	if m.AmountMinor() == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amountMinor: -m.amountMinor, currency: m.currency}, nil
}

// String formats the amount as "-123.45"-style decimal text, the inverse of
// ParseExternalAmount's accepted format (sign included, where applicable).
func (m Money) String() string {
	v := m.amountMinor
	sign := ""
	var mag uint64
	if v < 0 {
		sign = "-"
		// uint64(-v) is correct even when v == math.MinInt64: negating
		// MinInt64 as an int64 silently wraps back to MinInt64 (Go defines
		// overflow as wraparound, it does not panic), but reinterpreting
		// those same bits as uint64 yields exactly the right magnitude
		// (2^63), which is how two's complement works.
		mag = uint64(-v)
	} else {
		mag = uint64(v)
	}
	return fmt.Sprintf("%s%d.%02d", sign, mag/100, mag%100)
}

// moneyJSON is the wire shape required by the challenge: amount as a
// decimal string, never a JSON number (which would round-trip through a
// float in most decoders).
type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(moneyJSON{Amount: m.String(), Currency: m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var raw moneyJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := ParseExternalAmount(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

// Explicação, pensando em entrevista

// Por que int64 e não uma lib de decimal? O README permite as duas. int64 em centavos é mais barato (sem alocação extra por operação) e mais simples de auditar.
// O preço é que toda operação (soma, subtração, negação, parsing) precisa checar overflow manualmente — é o que Add/Negate/minorFromParts fazem.

// Por que duas formas de construir (NewMoney vs ParseExternalAmount)? Porque "dinheiro vindo de fora" e "dinheiro calculado internamente" têm regras diferentes:
// uma diferença de saldo pode ser negativa (é matemática interna), mas um valor que chega num POST /wagering/transactions nunca pode. Separar os dois construtores
// evita ter que lembrar "ah, mas só nesse caso aceita negativo" espalhado pelo código — a regra mora no nome da função que você chama.

// Por que uma regex só, em vez de cinco ifs? ^(\d+)(?:\.(\d{1,2}))?$ rejeita estruturalmente vazio, NaN, Infinity, notação científica, sinal e escala excedente,
// tudo de uma vez, porque nenhuma dessas formas bate com "só dígitos, opcionalmente um ponto com 1-2 dígitos depois, do começo ao fim da string". Se perguntarem "e se vier 1.5e3?" —
// o e não existe no padrão, a regex não casa, cai no ErrInvalidAmount antes de qualquer tentativa de interpretar número.

// A diferença proposital do wallet-go: lá, Money nunca sabe se formatar para o usuário — é regra do projeto que domínio não conhece apresentação. Aqui o contrato externo
// é {"amount":"25.00","currency":"BRL"} (§6.1 do README), e pedir pra uma camada de DTO decorar isso toda vez seria repetir a mesma lógica em todo handler. Decidi que aqui
// vale a troca: Money implementa MarshalJSON/UnmarshalJSON diretamente, reaproveitando o próprio ParseExternalAmount como parser do JSON. Se um entrevistador perguntar
// "isso não contradiz a regra de separar domínio e apresentação?" — a resposta é: a serialização aqui é o contrato do domínio, exigido pelo próprio enunciado, não uma formatação de UI.
