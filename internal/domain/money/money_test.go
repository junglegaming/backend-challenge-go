package money_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func minor(t *testing.T, v int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(v, "BRL")
	require.NoError(t, err)
	return m
}

func TestParseValid(t *testing.T) {
	t.Parallel()
	cases := map[string]int64{
		"0.00":                 0,
		"0.01":                 1,
		"0.10":                 10,
		"1.05":                 105,
		"25.00":                2500,
		"1000.99":              100099,
		"92233720368547757.00": 9223372036854775700,
		"92233720368547757.99": 9223372036854775799,
		"92233720368547758.07": math.MaxInt64,
	}
	for in, want := range cases {
		m := testutil.BRL(t, in)
		assert.Equal(t, want, m.Minor(), in)
		assert.Equal(t, money.Currency("BRL"), m.Currency())
		assert.Equal(t, in, m.Amount(), "round trip")
	}
}

func TestParseInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		amount, currency string
		want             error
	}{
		{"", "BRL", money.ErrInvalidAmount},
		{"NaN", "BRL", money.ErrInvalidAmount},
		{"Infinity", "BRL", money.ErrInvalidAmount},
		{"+Infinity", "BRL", money.ErrInvalidAmount},
		{"1e3", "BRL", money.ErrInvalidAmount},
		{"1.0e2", "BRL", money.ErrInvalidAmount},
		{"25", "BRL", money.ErrInvalidAmount},
		{"25.0", "BRL", money.ErrInvalidAmount},
		{"25.000", "BRL", money.ErrInvalidAmount},
		{"025.00", "BRL", money.ErrInvalidAmount},
		{" 25.00", "BRL", money.ErrInvalidAmount},
		{"25,00", "BRL", money.ErrInvalidAmount},
		{"+25.00", "BRL", money.ErrInvalidAmount},
		{".50", "BRL", money.ErrInvalidAmount},
		{"-25.00", "BRL", money.ErrNegativeAmount},
		{"-0.00", "BRL", money.ErrNegativeAmount},
		{"-abc", "BRL", money.ErrNegativeAmount}, // sign is checked before shape
		{"92233720368547758.08", "BRL", money.ErrOverflow},
		{"99999999999999999999999.00", "BRL", money.ErrOverflow},
		{"25.00", "", money.ErrInvalidCurrency},
		{"25.00", "brl", money.ErrInvalidCurrency},
		{"25.00", "XXX", money.ErrInvalidCurrency},
		{"25.00", "JPY", money.ErrInvalidCurrency},
	}
	for _, tc := range cases {
		_, err := money.Parse(tc.amount, tc.currency)
		assert.ErrorIs(t, err, tc.want, "%q %q", tc.amount, tc.currency)
	}
}

func TestParseMaxRoundTrip(t *testing.T) {
	t.Parallel()
	maxM := minor(t, math.MaxInt64)
	got, err := money.Parse(maxM.Amount(), "BRL")
	require.NoError(t, err)
	assert.Equal(t, maxM, got)
}

func TestFromMinor(t *testing.T) {
	t.Parallel()
	_, err := money.FromMinor(1, "XYZ")
	require.ErrorIs(t, err, money.ErrInvalidCurrency)
	_, err = money.FromMinor(math.MinInt64, "BRL")
	require.ErrorIs(t, err, money.ErrOverflow)

	neg := minor(t, -505)
	assert.Equal(t, "-5.05", neg.Amount())
	assert.Equal(t, "-5.05 BRL", neg.String())
	assert.Equal(t, "5.05", neg.Neg().Amount())
	assert.True(t, neg.IsNegative())
	assert.False(t, neg.IsPositive())
	assert.False(t, neg.IsZero())
}

func TestZero(t *testing.T) {
	t.Parallel()
	z, err := money.Zero("USD")
	require.NoError(t, err)
	assert.True(t, z.IsZero())
	assert.Equal(t, "0.00 USD", z.String())
	assert.Equal(t, z, z.Neg(), "negating zero is a no-op")
	_, err = money.Zero("nope")
	require.ErrorIs(t, err, money.ErrInvalidCurrency)
}

func TestArithmetic(t *testing.T) {
	t.Parallel()
	a, b := testutil.BRL(t, "100.00"), testutil.BRL(t, "80.00")
	sum, err := a.Add(b)
	require.NoError(t, err)
	assert.Equal(t, "180.00", sum.Amount())

	diff, err := b.Sub(a)
	require.NoError(t, err)
	assert.Equal(t, "-20.00", diff.Amount())

	assert.Equal(t, "-100.00", a.Neg().Amount())
	assert.Equal(t, a, a.Neg().Neg())
	assert.True(t, a.Equal(testutil.BRL(t, "100.00")))
	assert.False(t, a.Equal(b))
}

func TestCompare(t *testing.T) {
	t.Parallel()
	a, b := testutil.BRL(t, "1.00"), testutil.BRL(t, "2.00")
	cases := []struct {
		x, y money.Money
		want int
	}{{a, b, -1}, {b, a, 1}, {a, a, 0}}
	for _, tc := range cases {
		got, err := tc.x.Cmp(tc.y)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	t.Parallel()
	b := testutil.BRL(t, "1.00")
	usd, err := money.Parse("1.00", "USD")
	require.NoError(t, err)

	_, err = b.Add(usd)
	assert.ErrorIs(t, err, money.ErrCurrencyMismatch)
	_, err = b.Sub(usd)
	assert.ErrorIs(t, err, money.ErrCurrencyMismatch)
	_, err = b.Cmp(usd)
	assert.ErrorIs(t, err, money.ErrCurrencyMismatch)
	assert.False(t, b.Equal(usd), "same amount, different currency")
}

func TestUninitialized(t *testing.T) {
	t.Parallel()
	var zero money.Money
	valid := testutil.BRL(t, "1.00")
	require.ErrorIs(t, zero.Validate(), money.ErrUninitialized)
	require.NoError(t, valid.Validate())

	ops := []func() error{
		func() error { _, err := zero.Add(valid); return err },
		func() error { _, err := valid.Add(zero); return err },
		func() error { _, err := valid.Sub(zero); return err },
		func() error { _, err := zero.Sub(valid); return err },
		func() error { _, err := zero.Cmp(valid); return err },
	}
	for _, op := range ops {
		assert.ErrorIs(t, op(), money.ErrUninitialized)
	}
}

func TestOverflow(t *testing.T) {
	t.Parallel()
	maxM, minM, one := minor(t, math.MaxInt64), minor(t, math.MinInt64+1), minor(t, 1)
	ops := map[string]func() (money.Money, error){
		"max+1":     func() (money.Money, error) { return maxM.Add(one) },
		"min-1":     func() (money.Money, error) { return minM.Sub(one) },
		"min+(-1)":  func() (money.Money, error) { return minM.Add(one.Neg()) },
		"max-(min)": func() (money.Money, error) { return maxM.Sub(minM) },
	}
	for name, op := range ops {
		_, err := op()
		assert.ErrorIs(t, err, money.ErrOverflow, name)
	}

	got, err := maxM.Add(minor(t, 0))
	require.NoError(t, err)
	assert.Equal(t, maxM, got)
	got, err = minM.Neg().Add(minM)
	require.NoError(t, err)
	assert.True(t, got.IsZero())
}
