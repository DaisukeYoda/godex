package godex

import (
	"fmt"
	"time"
)

// ReconnectConfig tunes WebSocket reconnect behavior shared by all venue
// adapters: exponential backoff plus half-open detection.
type ReconnectConfig struct {
	// InitialDelay is the backoff delay after an unexpected drop; it resets
	// on every successful open.
	InitialDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
	// Multiplier scales the delay after each failed reconnect attempt.
	Multiplier float64
	// IdleTimeout is the maximum inbound silence (messages, pings, pongs)
	// before a connection is treated as half-open (e.g. a sleep/wake or
	// network partition where no close frame arrives) and force-reconnected.
	IdleTimeout time.Duration
}

// DefaultReconnectConfig returns the reference tuning used in production.
func DefaultReconnectConfig() ReconnectConfig {
	return ReconnectConfig{
		InitialDelay: 500 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		Multiplier:   2,
		IdleTimeout:  30 * time.Second,
	}
}

// IsZero reports whether c is the zero value.
func (c ReconnectConfig) IsZero() bool {
	return c == ReconnectConfig{}
}

// Validate rejects partially specified configs. Use the zero value (adapters
// substitute DefaultReconnectConfig) or specify every field.
func (c ReconnectConfig) Validate() error {
	if c.InitialDelay <= 0 || c.MaxDelay <= 0 || c.Multiplier <= 0 || c.IdleTimeout <= 0 {
		return fmt.Errorf("godex: reconnect config requires every field positive: %+v", c)
	}
	return nil
}
