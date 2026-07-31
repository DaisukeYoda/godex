package hyperliquid

import (
	"testing"

	"github.com/DaisukeYoda/godex/decimal"
)

func mustDecimal(t *testing.T, value string) decimal.Decimal {
	t.Helper()
	parsed, err := decimal.FromDecimalString(value)
	if err != nil {
		t.Fatalf("decimal %q: %v", value, err)
	}
	return parsed
}
