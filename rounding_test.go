package godex

import (
	"strings"
	"testing"

	"github.com/DaisukeYoda/godex/decimal"
)

func fd(t *testing.T, value string) decimal.Decimal {
	t.Helper()
	d, err := decimal.FromDecimalString(value)
	if err != nil {
		t.Fatalf("FromDecimalString(%q): %v", value, err)
	}
	return d
}

func TestRoundPriceToTick(t *testing.T) {
	tests := []struct {
		price, tick string
		side        Side
		want        string
	}{
		// Tick multiples pass through on both sides.
		{"150.12", "0.01", SideBuy, "150.12"},
		{"150.12", "0.01", SideSell, "150.12"},
		// Buys floor to the tick (dYdX SOL-USD testnet tick 0.01).
		{"150.1299", "0.01", SideBuy, "150.12"},
		{"150.1201", "0.01", SideBuy, "150.12"},
		// Sells ceil to the tick.
		{"150.1201", "0.01", SideSell, "150.13"},
		{"150.1299", "0.01", SideSell, "150.13"},
		// Halfway points still round by direction.
		{"150.125", "0.01", SideBuy, "150.12"},
		{"150.125", "0.01", SideSell, "150.13"},
		// Lighter SOL (price_decimals=3).
		{"150.1234", "0.001", SideBuy, "150.123"},
		{"150.1234", "0.001", SideSell, "150.124"},
		// Ticks above 1 round to multiples too.
		{"103", "5", SideBuy, "100"},
		{"103", "5", SideSell, "105"},
		// Price scale below tick scale.
		{"150", "0.01", SideBuy, "150.00"},
	}
	for _, tt := range tests {
		got, err := RoundPriceToTick(fd(t, tt.price), fd(t, tt.tick), tt.side)
		if err != nil {
			t.Fatalf("RoundPriceToTick(%s, %s, %s): %v", tt.price, tt.tick, tt.side, err)
		}
		if got.String() != tt.want {
			t.Fatalf("RoundPriceToTick(%s, %s, %s) = %s, want %s", tt.price, tt.tick, tt.side, got, tt.want)
		}
	}
}

func TestRoundPriceToTickRejectsNonPositiveInputs(t *testing.T) {
	if _, err := RoundPriceToTick(fd(t, "0"), fd(t, "0.01"), SideBuy); err == nil || !strings.Contains(err.Error(), "price must be positive") {
		t.Fatalf("got %v", err)
	}
	if _, err := RoundPriceToTick(fd(t, "150"), fd(t, "0"), SideBuy); err == nil || !strings.Contains(err.Error(), "tick must be positive") {
		t.Fatalf("got %v", err)
	}
	if _, err := RoundPriceToTick(fd(t, "-150"), fd(t, "0.01"), SideSell); err == nil || !strings.Contains(err.Error(), "price must be positive") {
		t.Fatalf("got %v", err)
	}
	if _, err := RoundPriceToTick(fd(t, "150"), fd(t, "0.01"), Side("hold")); err == nil || !strings.Contains(err.Error(), "invalid side") {
		t.Fatalf("got %v", err)
	}
}

func TestQuantizeSize(t *testing.T) {
	tests := []struct {
		size, step, minSize string
		want                string
	}{
		// Step multiples pass through.
		{"1.050", "0.001", "0.05", "1.050"},
		// Always floors (rounding up would exceed the intended position).
		{"1.0509", "0.001", "0.05", "1.050"}, // Lighter SOL: size_decimals=3, min_base 0.05
		{"1.0599", "0.001", "0.05", "1.059"},
		{"0.999", "0.01", "0.01", "0.99"}, // dYdX SOL-USD: stepSize 0.01
		// 0.0509 -> 0.050 exactly meets min 0.05.
		{"0.0509", "0.001", "0.05", "0.050"},
	}
	for _, tt := range tests {
		got, err := QuantizeSize(fd(t, tt.size), fd(t, tt.step), fd(t, tt.minSize))
		if err != nil {
			t.Fatalf("QuantizeSize(%s, %s, %s): %v", tt.size, tt.step, tt.minSize, err)
		}
		if got.String() != tt.want {
			t.Fatalf("QuantizeSize(%s, %s, %s) = %s, want %s", tt.size, tt.step, tt.minSize, got, tt.want)
		}
	}
}

func TestQuantizeSizeRejectsBelowMinimum(t *testing.T) {
	// 0.0499 -> 0.049 is below min 0.05: silently rounding up is forbidden.
	if _, err := QuantizeSize(fd(t, "0.0499"), fd(t, "0.001"), fd(t, "0.05")); err == nil || !strings.Contains(err.Error(), "below venue minimum") {
		t.Fatalf("got %v", err)
	}
	// A zero result is an error too.
	if _, err := QuantizeSize(fd(t, "0.009"), fd(t, "0.01"), fd(t, "0.01")); err == nil || !strings.Contains(err.Error(), "below venue minimum") {
		t.Fatalf("got %v", err)
	}
}

func TestQuantizeSizeRejectsNonPositiveInputs(t *testing.T) {
	if _, err := QuantizeSize(fd(t, "0"), fd(t, "0.01"), fd(t, "0.01")); err == nil || !strings.Contains(err.Error(), "size must be positive") {
		t.Fatalf("got %v", err)
	}
	if _, err := QuantizeSize(fd(t, "1"), fd(t, "0"), fd(t, "0.01")); err == nil || !strings.Contains(err.Error(), "step must be positive") {
		t.Fatalf("got %v", err)
	}
	if _, err := QuantizeSize(fd(t, "1"), fd(t, "0.01"), fd(t, "0")); err == nil || !strings.Contains(err.Error(), "minSize must be positive") {
		t.Fatalf("got %v", err)
	}
}

func TestSizeForNotional(t *testing.T) {
	tests := []struct {
		notional, price, step string
		want                  string
	}{
		// Floors to the largest step multiple not exceeding notional/price.
		{"1000.00", "150.00", "0.1", "6.6"},
		{"1000.00", "150.00", "0.025", "6.650"},
		// Remainders below one step yield zero.
		{"1.53", "76.22", "0.1", "0.0"},
		// A notional just below the boundary is not rounded up.
		{"7.621", "76.22", "0.1", "0.0"},
	}
	for _, tt := range tests {
		got, err := SizeForNotional(fd(t, tt.notional), fd(t, tt.price), fd(t, tt.step))
		if err != nil {
			t.Fatalf("SizeForNotional(%s, %s, %s): %v", tt.notional, tt.price, tt.step, err)
		}
		if got.String() != tt.want {
			t.Fatalf("SizeForNotional(%s, %s, %s) = %s, want %s", tt.notional, tt.price, tt.step, got, tt.want)
		}
	}
}

func TestQuantizeReduceOnlySize(t *testing.T) {
	tests := []struct {
		size, step string
		want       string
	}{
		// Ceils to the step, including sub-step dust.
		{"0.05", "0.1", "0.1"},
		{"1.01", "0.1", "1.1"},
		{"1.20", "0.1", "1.2"},
	}
	for _, tt := range tests {
		got, err := QuantizeReduceOnlySize(fd(t, tt.size), fd(t, tt.step))
		if err != nil {
			t.Fatalf("QuantizeReduceOnlySize(%s, %s): %v", tt.size, tt.step, err)
		}
		if got.String() != tt.want {
			t.Fatalf("QuantizeReduceOnlySize(%s, %s) = %s, want %s", tt.size, tt.step, got, tt.want)
		}
	}
}
