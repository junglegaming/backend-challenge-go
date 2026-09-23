// Package money implements the Money value object.
//
// Money is stored as a signed number of minor units (int64) plus an ISO 4217
// currency code. Every supported currency uses exactly two decimal places, so
// "25.00 BRL" is represented as 2500 minor units. Floating point types are
// never used: parsing, arithmetic and formatting are done on integers only.
//
// Limits: the representable range is [math.MinInt64+1, math.MaxInt64] minor
// units (about ±92 quadrillion major units). Every operation that could leave
// this range returns ErrOverflow instead of wrapping around.
package money

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Scale is the fixed number of decimal places of every supported currency.
// amountPattern and Amount are written for Scale == 2 and must change with it.
const Scale = 2

// minorPerMajor is 10^Scale, the number of minor units in one major unit.
var minorPerMajor = pow10(Scale)

// pow10 computes 10^n with integers only.
func pow10(n int) int64 {
	v := int64(1)
	for range n {
		v *= 10
	}
	return v
}

var (
	// ErrInvalidAmount reports a malformed decimal amount.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrNegativeAmount reports a negative amount on an external input.
	ErrNegativeAmount = errors.New("money: negative amount")
	// ErrInvalidCurrency reports an unknown or malformed currency code.
	ErrInvalidCurrency = errors.New("money: invalid currency")
	// ErrCurrencyMismatch reports arithmetic between different currencies.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrOverflow reports an int64 overflow.
	ErrOverflow = errors.New("money: overflow")
	// ErrUninitialized reports use of the zero value of Money.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// amountPattern accepts only plain non-negative decimals with exactly two
// fractional digits and no superfluous leading zeros ("0.50", "25.00").
var amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.([0-9]{2})$`)

// Currency is an ISO 4217 alphabetic code.
type Currency string

// supportedCurrencies lists ISO 4217 codes whose minor unit is 2.
var supportedCurrencies = map[Currency]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {}, "CAD": {},
}

// ParseCurrency validates an ISO 4217 code.
func ParseCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := supportedCurrencies[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return c, nil
}

// Money is an immutable amount of a currency. The zero value is invalid.
type Money struct {
	minor    int64
	currency Currency
}

// Parse builds Money from an external decimal string such as "25.00".
// Empty values, NaN, Infinity, exponents, signs, missing or extra scale and
// out-of-range values are rejected; nothing is rounded.
func Parse(amount, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	if strings.HasPrefix(amount, "-") {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	m := amountPattern.FindStringSubmatch(amount)
	if m == nil {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	minor, err := combine(m[1], m[2])
	if err != nil {
		return Money{}, fmt.Errorf("%w: %q", err, amount)
	}
	return Money{minor: minor, currency: c}, nil
}

// combine joins the integer and fractional digits into minor units. The
// bound is exact: the largest accepted value is math.MaxInt64 itself.
func combine(integer, fraction string) (int64, error) {
	major, err := strconv.ParseInt(integer, 10, 64)
	if err != nil {
		return 0, ErrOverflow
	}
	frac, _ := strconv.ParseInt(fraction, 10, 64) // regex guarantees two digits
	maxMajor, maxFrac := math.MaxInt64/minorPerMajor, math.MaxInt64%minorPerMajor
	if major > maxMajor || (major == maxMajor && frac > maxFrac) {
		return 0, ErrOverflow
	}
	return major*minorPerMajor + frac, nil
}

// FromMinor builds Money from minor units. Negative values are allowed for
// internal differences; math.MinInt64 is rejected so Neg never overflows.
func FromMinor(minor int64, currency Currency) (Money, error) {
	if _, err := ParseCurrency(string(currency)); err != nil {
		return Money{}, err
	}
	if minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: minor, currency: currency}, nil
}

// Zero returns zero in the given currency.
func Zero(currency Currency) (Money, error) {
	return FromMinor(0, currency)
}

// Minor returns the amount in minor units.
func (m Money) Minor() int64 { return m.minor }

// Currency returns the currency code.
func (m Money) Currency() Currency { return m.currency }

// IsZero reports whether the amount is zero.
func (m Money) IsZero() bool { return m.minor == 0 }

// IsPositive reports whether the amount is greater than zero.
func (m Money) IsPositive() bool { return m.minor > 0 }

// IsNegative reports whether the amount is lower than zero.
func (m Money) IsNegative() bool { return m.minor < 0 }

// Validate rejects the zero value of Money.
func (m Money) Validate() error {
	if m.currency == "" {
		return ErrUninitialized
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	sum, err := addInt64(m.minor, o.minor)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: sum, currency: m.currency}, nil
}

// addInt64 adds with overflow detection; math.MinInt64 counts as overflow
// because it has no positive counterpart.
func addInt64(a, b int64) (int64, error) {
	sum := a + b
	if (b > 0) != (sum > a) && b != 0 {
		return 0, ErrOverflow
	}
	if sum == math.MinInt64 {
		return 0, ErrOverflow
	}
	return sum, nil
}

// Sub returns m - o. Add already rejects uninitialized operands.
func (m Money) Sub(o Money) (Money, error) {
	return m.Add(o.Neg())
}

// Neg returns -m. It cannot overflow because math.MinInt64 is never stored.
func (m Money) Neg() Money {
	return Money{minor: -m.minor, currency: m.currency}
}

// Cmp compares m and o, returning -1, 0 or +1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both values have the same amount and currency.
func (m Money) Equal(o Money) bool { return m == o }

func (m Money) compatible(o Money) error {
	if m.Validate() != nil || o.Validate() != nil {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s != %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Amount formats the amount as a fixed-scale decimal string ("-5.00").
func (m Money) Amount() string {
	sign := ""
	v := m.minor
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%02d", sign, v/minorPerMajor, v%minorPerMajor)
}

// String implements fmt.Stringer.
func (m Money) String() string {
	return m.Amount() + " " + string(m.currency)
}
