package godex

import (
	"fmt"
	"math/big"

	"github.com/DaisukeYoda/godex/decimal"
)

// Rounding to venue tick/step sizes (pure functions). The rounding direction
// is always conservative:
//
//   - Price: buy floors, sell ceils. For post-only this moves away from the
//     opposing quote (no accidental taker fills); for IOC the price is a
//     slippage cap, so buys never raise it and sells never lower it. The
//     direction is the same for both intents, so rounding ignores intent.
//   - Size: always floors (never exceed the intended position). If the result
//     falls below the venue minimum, that is an error — silently rounding up
//     is forbidden.
//   - Reduce-only size: the venue prevents position flips, so dust may be
//     ceiled up to one step to fully close.

// pow10 returns 10^exponent. exponent must be non-negative.
func pow10(exponent int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}

func assertOrderSide(side Side) error {
	if side != SideBuy && side != SideSell {
		return fmt.Errorf("godex: invalid side %q", side)
	}
	return nil
}

func assertPositive(value decimal.Decimal, label string) error {
	if value.Sign() <= 0 {
		return fmt.Errorf("godex: %s must be positive: %s", label, value)
	}
	return nil
}

// liftToCommonScale returns both mantissas lifted exactly to the larger scale.
func liftToCommonScale(a, b decimal.Decimal) (aMantissa, bMantissa *big.Int, scale int) {
	scale = max(a.Scale(), b.Scale())
	return a.Rescale(scale).Mantissa(), b.Rescale(scale).Mantissa(), scale
}

// RoundPriceToTick rounds price to a multiple of tick: buy floors, sell
// ceils. The result carries tick's scale.
func RoundPriceToTick(price, tick decimal.Decimal, side Side) (decimal.Decimal, error) {
	if err := assertOrderSide(side); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(price, "price"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(tick, "tick"); err != nil {
		return decimal.Decimal{}, err
	}
	priceMantissa, tickMantissa, scale := liftToCommonScale(price, tick)
	quotient, remainder := new(big.Int).QuoRem(priceMantissa, tickMantissa, new(big.Int))
	// Values are positive, so big.Int truncation equals floor for buys.
	if side == SideSell && remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	rounded := decimal.FromBigInt(quotient.Mul(quotient, tickMantissa), scale)
	// The mantissa is a tick multiple, so shrinking to tick's scale is exact.
	return rounded.Rescale(tick.Scale()), nil
}

// QuantizeSize floors size to a multiple of step. If the result is zero or
// below minSize, it returns an error rather than silently rounding up.
func QuantizeSize(size, step, minSize decimal.Decimal) (decimal.Decimal, error) {
	if err := assertPositive(size, "size"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(step, "step"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(minSize, "minSize"); err != nil {
		return decimal.Decimal{}, err
	}
	sizeMantissa, stepMantissa, scale := liftToCommonScale(size, step)
	steps := new(big.Int).Quo(sizeMantissa, stepMantissa)
	quantized := decimal.FromBigInt(steps.Mul(steps, stepMantissa), scale).Rescale(step.Scale())
	if quantized.IsZero() || quantized.Cmp(minSize) < 0 {
		return decimal.Decimal{}, fmt.Errorf(
			"godex: quantized size %s is below venue minimum %s (input %s, step %s)",
			quantized, minSize, size, step)
	}
	return quantized, nil
}

// QuantizeReduceOnlySize ceils size to a multiple of step. Reduce-only orders
// cannot flip the position, so dust may be ceiled up to fully close.
func QuantizeReduceOnlySize(size, step decimal.Decimal) (decimal.Decimal, error) {
	if err := assertPositive(size, "size"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(step, "step"); err != nil {
		return decimal.Decimal{}, err
	}
	sizeMantissa, stepMantissa, scale := liftToCommonScale(size, step)
	quotient, remainder := new(big.Int).QuoRem(sizeMantissa, stepMantissa, new(big.Int))
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return decimal.FromBigInt(quotient.Mul(quotient, stepMantissa), scale).Rescale(step.Scale()), nil
}

// SizeForNotional returns the largest step multiple not exceeding
// notional / price, computed without floating point. The result may be zero.
func SizeForNotional(notional, price, step decimal.Decimal) (decimal.Decimal, error) {
	if err := assertPositive(notional, "notional"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(price, "price"); err != nil {
		return decimal.Decimal{}, err
	}
	if err := assertPositive(step, "step"); err != nil {
		return decimal.Decimal{}, err
	}
	exponent := price.Scale() + step.Scale() - notional.Scale()
	numerator := notional.Mantissa()
	denominator := new(big.Int).Mul(price.Mantissa(), step.Mantissa())
	if exponent >= 0 {
		numerator.Mul(numerator, pow10(exponent))
	} else {
		denominator.Mul(denominator, pow10(-exponent))
	}
	steps := new(big.Int).Quo(numerator, denominator)
	return decimal.FromBigInt(steps.Mul(steps, step.Mantissa()), step.Scale()), nil
}
