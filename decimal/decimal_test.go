package decimal

import (
	"math/big"
	"testing"
)

func mustParse(t *testing.T, value string, scale int) Decimal {
	t.Helper()
	d, err := FromString(value, scale)
	if err != nil {
		t.Fatalf("FromString(%q, %d): %v", value, scale, err)
	}
	return d
}

func assertDecimal(t *testing.T, got Decimal, wantMantissa int64, wantScale int) {
	t.Helper()
	if got.mant().Cmp(big.NewInt(wantMantissa)) != 0 || got.scale != wantScale {
		t.Fatalf("got {%s, %d}, want {%d, %d}", got.mant(), got.scale, wantMantissa, wantScale)
	}
}

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func TestFromString(t *testing.T) {
	tests := []struct {
		value        string
		scale        int
		wantMantissa int64
	}{
		{"100001.5", 1, 1000015}, // integer and fractional parts combine into the mantissa
		{"42", 2, 4200},          // missing fraction is zero-padded
		{"-0.05", 2, -5},         // negative values
		{"97000", 0, 97000},      // scale 0 represents integers
		{"-0.00", 2, 0},          // zero is normalized (no negative zero)
		{"1.500", 1, 15},         // excess digits allowed when they are trailing zeros
	}
	for _, tt := range tests {
		assertDecimal(t, mustParse(t, tt.value, tt.scale), tt.wantMantissa, tt.scale)
	}
}

func TestFromStringRejectsSignificantDigitsBeyondScale(t *testing.T) {
	for _, tt := range []struct {
		value string
		scale int
	}{{"1.55", 1}, {"0.001", 2}} {
		if _, err := FromString(tt.value, tt.scale); err == nil {
			t.Fatalf("FromString(%q, %d): expected error", tt.value, tt.scale)
		}
	}
}

func TestFromStringRejectsInvalidFormats(t *testing.T) {
	for _, value := range []string{"abc", "1.", ".5", "1e3", "+1", "1,000", ""} {
		if _, err := FromString(value, 2); err == nil {
			t.Fatalf("FromString(%q): expected error", value)
		}
	}
}

func TestFromStringPanicsOnNegativeScale(t *testing.T) {
	expectPanic(t, func() { _, _ = FromString("1", -1) })
}

func TestFromStringRounded(t *testing.T) {
	// Within scale: identical to FromString.
	assertDecimal(t, must(FromStringRounded("1234567.8", 2)), 123456780, 2)
	assertDecimal(t, must(FromStringRounded("42", 2)), 4200, 2)
	// Beyond scale: round half away from zero.
	assertDecimal(t, must(FromStringRounded("1.005", 2)), 101, 2)
	assertDecimal(t, must(FromStringRounded("1.004", 2)), 100, 2)
	assertDecimal(t, must(FromStringRounded("-1.005", 2)), -101, 2)
	// 12-digit funding rate ingested at scale 8.
	if got := must(FromStringRounded("0.000012345678", 8)).String(); got != "0.00001235" {
		t.Fatalf("got %q", got)
	}
	if _, err := FromStringRounded("1e3", 2); err == nil {
		t.Fatal("expected error for invalid format")
	}
	expectPanic(t, func() { _, _ = FromStringRounded("1", -1) })
}

func must(d Decimal, err error) Decimal {
	if err != nil {
		panic(err)
	}
	return d
}

func TestFromDecimalString(t *testing.T) {
	assertDecimal(t, must(FromDecimalString("100001.5")), 1000015, 1)
	assertDecimal(t, must(FromDecimalString("-0.05")), -5, 2)
	assertDecimal(t, must(FromDecimalString("97000")), 97000, 0)
	if _, err := FromDecimalString("1e3"); err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestNew(t *testing.T) {
	assertDecimal(t, New(1, 2), 1, 2)
	if got := New(1, 2).String(); got != "0.01" {
		t.Fatalf("got %q", got)
	}
	expectPanic(t, func() { New(1, -1) })
}

func TestRescale(t *testing.T) {
	// Increasing the scale is exact.
	assertDecimal(t, mustParse(t, "1.5", 1).Rescale(3), 1500, 3)
	// Same scale returns an equal value.
	assertDecimal(t, mustParse(t, "1.5", 1).Rescale(1), 15, 1)
	// Decreasing the scale rounds half away from zero.
	assertDecimal(t, mustParse(t, "1.24", 2).Rescale(1), 12, 1)
	assertDecimal(t, mustParse(t, "1.25", 2).Rescale(1), 13, 1)
	assertDecimal(t, mustParse(t, "1.26", 2).Rescale(1), 13, 1)
	// Negative values round away from zero.
	assertDecimal(t, mustParse(t, "-1.25", 2).Rescale(1), -13, 1)
	assertDecimal(t, mustParse(t, "-1.24", 2).Rescale(1), -12, 1)
}

func TestAddSub(t *testing.T) {
	assertDecimal(t, mustParse(t, "100001.5", 1).Add(mustParse(t, "0.5", 1)), 1000020, 1)
	assertDecimal(t, mustParse(t, "1.0", 1).Sub(mustParse(t, "2.5", 1)), -15, 1)
	// Scale mismatch panics — no implicit conversion.
	expectPanic(t, func() { mustParse(t, "1", 1).Add(mustParse(t, "1", 2)) })
	expectPanic(t, func() { mustParse(t, "1", 1).Sub(mustParse(t, "1", 2)) })
}

func TestMulToScale(t *testing.T) {
	price := mustParse(t, "100001.5", 1)
	size := mustParse(t, "0.5", 1)
	assertDecimal(t, price.MulToScale(size, 2), 5000075, 2)
	assertDecimal(t, price.MulToScale(size, 1), 500008, 1)
	// Result scale above the intermediate scale stays exact.
	assertDecimal(t, mustParse(t, "1.5", 1).MulToScale(mustParse(t, "2", 0), 3), 3000, 3)
	// Signs.
	assertDecimal(t, mustParse(t, "-1.5", 1).MulToScale(mustParse(t, "2", 0), 1), -30, 1)
	// Fee example: $1,000,000 x 0.045% = $450.
	notional := mustParse(t, "1000000", 2)
	feeRate := mustParse(t, "0.00045", 6)
	if got := notional.MulToScale(feeRate, 2).String(); got != "450.00" {
		t.Fatalf("got %q", got)
	}
}

func TestDivToScale(t *testing.T) {
	assertDecimal(t, mustParse(t, "1", 0).DivToScale(mustParse(t, "3", 0), 4), 3333, 4)
	assertDecimal(t, mustParse(t, "2", 0).DivToScale(mustParse(t, "3", 0), 4), 6667, 4)
	// Mixed scales.
	assertDecimal(t, mustParse(t, "100.0", 1).DivToScale(mustParse(t, "8", 0), 2), 1250, 2)
	// Negative values round away from zero.
	assertDecimal(t, mustParse(t, "-1", 0).DivToScale(mustParse(t, "8", 0), 2), -13, 2)
	// Division by zero panics.
	expectPanic(t, func() { mustParse(t, "1", 0).DivToScale(mustParse(t, "0", 2), 2) })
	// Average execution price: $500,007.5 / 5 BTC = $100,001.5.
	cost := mustParse(t, "500007.5", 1)
	size := mustParse(t, "5.00000", 5)
	if got := cost.DivToScale(size, 1).String(); got != "100001.5" {
		t.Fatalf("got %q", got)
	}
}

func TestCmpMinMax(t *testing.T) {
	if got := mustParse(t, "1.5", 1).Cmp(mustParse(t, "1.5", 1)); got != 0 {
		t.Fatalf("got %d", got)
	}
	if got := mustParse(t, "1.4", 1).Cmp(mustParse(t, "1.5", 1)); got != -1 {
		t.Fatalf("got %d", got)
	}
	if got := mustParse(t, "1.6", 1).Cmp(mustParse(t, "1.5", 1)); got != 1 {
		t.Fatalf("got %d", got)
	}
	// Cross-scale comparison is exact.
	if got := mustParse(t, "1.5", 1).Cmp(mustParse(t, "1.50", 2)); got != 0 {
		t.Fatalf("got %d", got)
	}
	if got := mustParse(t, "1.5", 1).Cmp(mustParse(t, "1.49", 2)); got != 1 {
		t.Fatalf("got %d", got)
	}
	if got := mustParse(t, "-1.5", 1).Cmp(mustParse(t, "-1.49", 2)); got != -1 {
		t.Fatalf("got %d", got)
	}
	// Min/Max return the first operand on ties.
	a := mustParse(t, "1.5", 1)
	b := mustParse(t, "1.50", 2)
	if got := Min(a, b); got.scale != a.scale {
		t.Fatalf("Min tie: got scale %d", got.scale)
	}
	if got := Max(a, b); got.scale != a.scale {
		t.Fatalf("Max tie: got scale %d", got.scale)
	}
	if got := Min(mustParse(t, "1.4", 1), b); got.String() != "1.4" {
		t.Fatalf("got %q", got)
	}
	if got := Max(mustParse(t, "1.4", 1), b); got.String() != "1.50" {
		t.Fatalf("got %q", got)
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		value string
		scale int
	}{
		{"100001.5", 1},
		{"-0.05", 2},
		{"0.00", 2},
		{"97000", 0},
	}
	// Round-trips with FromString.
	for _, tt := range tests {
		if got := mustParse(t, tt.value, tt.scale).String(); got != tt.value {
			t.Fatalf("got %q, want %q", got, tt.value)
		}
	}
	if got := New(42, 0).String(); got != "42" {
		t.Fatalf("got %q", got)
	}
}

func TestZeroValue(t *testing.T) {
	var zero Decimal
	if !zero.IsZero() || zero.Sign() != 0 || zero.Scale() != 0 {
		t.Fatal("zero value must behave as 0 at scale 0")
	}
	if got := zero.String(); got != "0" {
		t.Fatalf("got %q", got)
	}
	if got := zero.Cmp(mustParse(t, "0.00", 2)); got != 0 {
		t.Fatalf("got %d", got)
	}
	assertDecimal(t, zero.Add(New(5, 0)), 5, 0)
}

func TestMantissaIsDefensiveCopy(t *testing.T) {
	d := mustParse(t, "1.5", 1)
	d.Mantissa().SetInt64(999)
	if got := d.String(); got != "1.5" {
		t.Fatalf("mutating Mantissa() copy leaked into the value: %q", got)
	}
}
