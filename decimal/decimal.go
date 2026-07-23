// Package decimal provides an immutable fixed-point decimal:
// value = mantissa / 10^scale.
//
// Design rules (ported from the reference implementation, decision D1):
//   - Construction is string-only. No float constructors exist, closing the
//     rounding-error injection path.
//   - Add/Sub require equal scales. There is no implicit rescaling; callers
//     must call Rescale explicitly.
//   - Reducing scale always rounds half away from zero.
//
// Untrusted input is parsed via the FromString family, which returns errors
// (fail fast at the boundary). Contract violations — scale mismatch, negative
// scale, division by zero — are programming bugs and panic, following
// math/big conventions.
package decimal

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

// Decimal is an immutable fixed-point decimal. The zero value is 0 at scale 0.
type Decimal struct {
	mantissa *big.Int
	scale    int
}

var (
	bigZero = big.NewInt(0)
	bigOne  = big.NewInt(1)
	bigTen  = big.NewInt(10)

	decimalPattern = regexp.MustCompile(`^(-?)(\d+)(?:\.(\d+))?$`)
)

// pow10 returns 10^exponent. exponent must be non-negative.
func pow10(exponent int) *big.Int {
	return new(big.Int).Exp(bigTen, big.NewInt(int64(exponent)), nil)
}

// mant returns the mantissa, normalizing the zero value's nil pointer.
// Callers must never mutate the returned value.
func (d Decimal) mant() *big.Int {
	if d.mantissa == nil {
		return bigZero
	}
	return d.mantissa
}

func assertValidScale(scale int) {
	if scale < 0 {
		panic(fmt.Sprintf("decimal: scale must be non-negative: %d", scale))
	}
}

func assertSameScale(a, b Decimal, op string) {
	if a.scale != b.scale {
		panic(fmt.Sprintf("decimal: %s: scale mismatch (%d != %d); call Rescale explicitly", op, a.scale, b.scale))
	}
}

// divideRounded divides rounding half away from zero. denominator must be positive.
func divideRounded(numerator, denominator *big.Int) *big.Int {
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() == 0 {
		return quotient
	}
	absRemainderDoubled := new(big.Int).Abs(remainder)
	absRemainderDoubled.Lsh(absRemainderDoubled, 1)
	if absRemainderDoubled.Cmp(denominator) >= 0 {
		if numerator.Sign() < 0 {
			quotient.Sub(quotient, bigOne)
		} else {
			quotient.Add(quotient, bigOne)
		}
	}
	return quotient
}

// FromString parses a decimal string at the given scale. Significant digits
// beyond the scale are an error (fail fast); excess trailing zeros ("1.500"
// at scale 1) lose no information and are accepted.
func FromString(value string, scale int) (Decimal, error) {
	assertValidScale(scale)
	match := decimalPattern.FindStringSubmatch(value)
	if match == nil {
		return Decimal{}, fmt.Errorf("decimal: invalid decimal string %q", value)
	}
	intPart, fracPart := match[2], match[3]
	if len(fracPart) > scale && strings.ContainsAny(fracPart[scale:], "123456789") {
		return Decimal{}, fmt.Errorf("decimal: %q has significant digits beyond scale %d; rescale explicitly instead", value, scale)
	}
	frac := fracPart
	if len(frac) > scale {
		frac = frac[:scale]
	}
	frac += strings.Repeat("0", scale-len(frac))
	mantissa, ok := new(big.Int).SetString(intPart+frac, 10)
	if !ok {
		return Decimal{}, fmt.Errorf("decimal: invalid decimal string %q", value)
	}
	if match[1] == "-" {
		mantissa.Neg(mantissa)
	}
	return Decimal{mantissa: mantissa, scale: scale}, nil
}

// FromStringRounded parses a decimal string of unknown native precision,
// rounding digits beyond the scale half away from zero. Intended only for
// ingesting venue statistics (volume, open interest, ...); order-book prices
// and sizes must use FromString, which rejects precision overflow.
func FromStringRounded(value string, scale int) (Decimal, error) {
	assertValidScale(scale)
	match := decimalPattern.FindStringSubmatch(value)
	if match == nil {
		return Decimal{}, fmt.Errorf("decimal: invalid decimal string %q", value)
	}
	nativeScale := len(match[3])
	if nativeScale <= scale {
		return FromString(value, scale)
	}
	exact, err := FromString(value, nativeScale)
	if err != nil {
		return Decimal{}, err
	}
	return exact.Rescale(scale), nil
}

// FromDecimalString parses a decimal string using its fractional digit count
// as the scale — the exact inverse of String().
func FromDecimalString(value string) (Decimal, error) {
	match := decimalPattern.FindStringSubmatch(value)
	if match == nil {
		return Decimal{}, fmt.Errorf("decimal: invalid decimal string %q", value)
	}
	return FromString(value, len(match[3]))
}

// MustFromString is FromString that panics on error. For constants and tests.
func MustFromString(value string, scale int) Decimal {
	d, err := FromString(value, scale)
	if err != nil {
		panic(err)
	}
	return d
}

// New returns mantissa / 10^scale, e.g. New(1, 2) == 0.01.
func New(mantissa int64, scale int) Decimal {
	assertValidScale(scale)
	return Decimal{mantissa: big.NewInt(mantissa), scale: scale}
}

// FromBigInt returns mantissa / 10^scale, copying mantissa defensively.
func FromBigInt(mantissa *big.Int, scale int) Decimal {
	assertValidScale(scale)
	return Decimal{mantissa: new(big.Int).Set(mantissa), scale: scale}
}

// Rescale converts d to the given scale. Increasing the scale is exact;
// decreasing it rounds half away from zero.
func (d Decimal) Rescale(scale int) Decimal {
	assertValidScale(scale)
	if scale == d.scale {
		return d
	}
	if scale > d.scale {
		return Decimal{mantissa: new(big.Int).Mul(d.mant(), pow10(scale-d.scale)), scale: scale}
	}
	return Decimal{mantissa: divideRounded(d.mant(), pow10(d.scale-scale)), scale: scale}
}

// Add returns d + o. Panics if scales differ.
func (d Decimal) Add(o Decimal) Decimal {
	assertSameScale(d, o, "Add")
	return Decimal{mantissa: new(big.Int).Add(d.mant(), o.mant()), scale: d.scale}
}

// Sub returns d - o. Panics if scales differ.
func (d Decimal) Sub(o Decimal) Decimal {
	assertSameScale(d, o, "Sub")
	return Decimal{mantissa: new(big.Int).Sub(d.mant(), o.mant()), scale: d.scale}
}

// MulToScale returns d * o rounded to the given scale. The intermediate
// product is held exactly.
func (d Decimal) MulToScale(o Decimal, scale int) Decimal {
	assertValidScale(scale)
	raw := new(big.Int).Mul(d.mant(), o.mant())
	rawScale := d.scale + o.scale
	if scale >= rawScale {
		return Decimal{mantissa: raw.Mul(raw, pow10(scale-rawScale)), scale: scale}
	}
	return Decimal{mantissa: divideRounded(raw, pow10(rawScale-scale)), scale: scale}
}

// DivToScale returns d / o rounded to the given scale. Panics if o is zero.
func (d Decimal) DivToScale(o Decimal, scale int) Decimal {
	assertValidScale(scale)
	if o.mant().Sign() == 0 {
		panic("decimal: division by zero")
	}
	exponent := scale + o.scale - d.scale
	numerator := new(big.Int).Set(d.mant())
	denominator := new(big.Int).Set(o.mant())
	if exponent >= 0 {
		numerator.Mul(numerator, pow10(exponent))
	} else {
		denominator.Mul(denominator, pow10(-exponent))
	}
	if denominator.Sign() < 0 {
		numerator.Neg(numerator)
		denominator.Neg(denominator)
	}
	return Decimal{mantissa: divideRounded(numerator, denominator), scale: scale}
}

// Cmp compares d and o as numbers, lifting to a common scale exactly, so
// operands of different scales compare correctly.
func (d Decimal) Cmp(o Decimal) int {
	left, right := d.mant(), o.mant()
	if d.scale < o.scale {
		left = new(big.Int).Mul(left, pow10(o.scale-d.scale))
	} else if o.scale < d.scale {
		right = new(big.Int).Mul(right, pow10(d.scale-o.scale))
	}
	return left.Cmp(right)
}

// Min returns the smaller of a and b (a when equal).
func Min(a, b Decimal) Decimal {
	if a.Cmp(b) <= 0 {
		return a
	}
	return b
}

// Max returns the larger of a and b (a when equal).
func Max(a, b Decimal) Decimal {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}

// String returns the canonical decimal representation, always emitting
// exactly scale fractional digits.
func (d Decimal) String() string {
	mant := d.mant()
	digits := new(big.Int).Abs(mant).String()
	if pad := d.scale + 1 - len(digits); pad > 0 {
		digits = strings.Repeat("0", pad) + digits
	}
	body := digits[:len(digits)-d.scale]
	if d.scale > 0 {
		body += "." + digits[len(digits)-d.scale:]
	}
	if mant.Sign() < 0 {
		return "-" + body
	}
	return body
}

// Mantissa returns a defensive copy of the mantissa.
func (d Decimal) Mantissa() *big.Int {
	return new(big.Int).Set(d.mant())
}

// Scale returns the scale.
func (d Decimal) Scale() int {
	return d.scale
}

// IsZero reports whether d equals zero at any scale.
func (d Decimal) IsZero() bool {
	return d.mant().Sign() == 0
}

// Sign returns -1, 0, or +1 depending on the sign of d.
func (d Decimal) Sign() int {
	return d.mant().Sign()
}
