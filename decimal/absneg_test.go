package decimal

import "testing"

// Tests for Abs and Neg: immutable sign-only transforms that preserve scale.

func TestAbs(t *testing.T) {
	tests := []struct {
		value        string
		scale        int
		wantMantissa int64
	}{
		{"1.5", 1, 15},       // positive is unchanged
		{"-1.5", 1, 15},      // negative flips to positive
		{"0.00", 2, 0},       // zero stays zero
		{"-0.05", 2, 5},      // scale is preserved through the sign flip
		{"-97000", 0, 97000}, // scale 0 integers
	}
	for _, tt := range tests {
		assertDecimal(t, mustParse(t, tt.value, tt.scale).Abs(), tt.wantMantissa, tt.scale)
	}
}

func TestNeg(t *testing.T) {
	tests := []struct {
		value        string
		scale        int
		wantMantissa int64
	}{
		{"1.5", 1, -15},      // positive becomes negative
		{"-1.5", 1, 15},      // negative becomes positive
		{"0.00", 2, 0},       // zero stays zero (no negative zero)
		{"-0.05", 2, 5},      // scale is preserved through the sign flip
		{"97000", 0, -97000}, // scale 0 integers
	}
	for _, tt := range tests {
		assertDecimal(t, mustParse(t, tt.value, tt.scale).Neg(), tt.wantMantissa, tt.scale)
	}
}

func TestAbsNegImmutable(t *testing.T) {
	original := mustParse(t, "-1.5", 1)
	abs := original.Abs()
	neg := original.Neg()
	// The receiver is untouched by either call.
	assertDecimal(t, original, -15, 1)
	// The results share no mantissa with the receiver (white-box aliasing check).
	abs.mantissa.SetInt64(999)
	neg.mantissa.SetInt64(999)
	assertDecimal(t, original, -15, 1)
}

func TestAbsNegZeroValue(t *testing.T) {
	var zero Decimal
	assertDecimal(t, zero.Abs(), 0, 0)
	assertDecimal(t, zero.Neg(), 0, 0)
	// The package-shared zero mantissa must survive both calls unmutated.
	assertDecimal(t, zero.Add(New(5, 0)), 5, 0)
}
