package godex

import (
	"github.com/DaisukeYoda/godex/decimal"
)

// ComputeMarginUsage returns (total - available) / total at MarginUsageScale.
// Venue adapters map their native wire fields onto (total, available) — e.g.
// equity/freeCollateral or collateral/availableBalance. Zero total (an
// unfunded account) is zero usage.
func ComputeMarginUsage(total, available string) (decimal.Decimal, error) {
	totalD, err := decimal.FromDecimalString(total)
	if err != nil {
		return decimal.Decimal{}, err
	}
	if totalD.IsZero() {
		return decimal.New(0, MarginUsageScale), nil
	}
	availableD, err := decimal.FromDecimalString(available)
	if err != nil {
		return decimal.Decimal{}, err
	}
	scale := max(totalD.Scale(), availableD.Scale())
	used := totalD.Rescale(scale).Sub(availableD.Rescale(scale))
	return used.DivToScale(totalD.Rescale(scale), MarginUsageScale), nil
}
