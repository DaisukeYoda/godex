package godex

import (
	"testing"
)

func TestComputeMarginUsage(t *testing.T) {
	tests := []struct {
		total, available string
		want             string
	}{
		{"1000.00", "400.00", "0.6000"},
		{"1000", "1000", "0.0000"},
		// Zero total (unfunded account) is zero usage, not division by zero.
		{"0", "0", "0.0000"},
		// Mixed native scales.
		{"1000.5", "250.125", "0.7500"},
	}
	for _, tt := range tests {
		got, err := ComputeMarginUsage(tt.total, tt.available)
		if err != nil {
			t.Fatalf("ComputeMarginUsage(%q, %q): %v", tt.total, tt.available, err)
		}
		if got.String() != tt.want {
			t.Fatalf("ComputeMarginUsage(%q, %q) = %s, want %s", tt.total, tt.available, got, tt.want)
		}
	}
}

func TestComputeMarginUsageRejectsInvalidInput(t *testing.T) {
	if _, err := ComputeMarginUsage("abc", "1"); err == nil {
		t.Fatal("expected error for invalid total")
	}
	if _, err := ComputeMarginUsage("1", "abc"); err == nil {
		t.Fatal("expected error for invalid available")
	}
}

func TestReconnectConfig(t *testing.T) {
	if err := DefaultReconnectConfig().Validate(); err != nil {
		t.Fatalf("default config must validate: %v", err)
	}
	if !(ReconnectConfig{}).IsZero() {
		t.Fatal("zero value must report IsZero")
	}
	partial := ReconnectConfig{InitialDelay: 1}
	if err := partial.Validate(); err == nil {
		t.Fatal("partial config must fail validation")
	}
}
