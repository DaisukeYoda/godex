package dydx

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// heightTracker caches the chain's latest block height.
//
// Every short-term order carries a good_til_block derived from the current
// height, so the height is as load-bearing here as a nonce is elsewhere: it must
// be known before Connect returns, and a submission must never guess it. An
// under-estimate produces an order the chain drops as already expired while the
// broadcast still looks accepted; an over-estimate is rejected outright. So a
// height that has not been refreshed recently is treated as unknown rather than
// extrapolated from elapsed time.
//
// The tracker is safe for concurrent use.
type heightTracker struct {
	fetch      func(ctx context.Context) (uint32, error)
	staleAfter time.Duration
	now        func() time.Time

	mu        sync.Mutex
	height    uint32
	fetchedAt time.Time
}

func newHeightTracker(
	fetch func(ctx context.Context) (uint32, error),
	staleAfter time.Duration,
	now func() time.Time,
) *heightTracker {
	return &heightTracker{fetch: fetch, staleAfter: staleAfter, now: now}
}

// refresh fetches the latest height and records the observation time. Callers
// after the first (the poll loop) may log and ignore failures; staleness, not a
// single failed poll, is what stops trading.
func (h *heightTracker) refresh(ctx context.Context) error {
	height, err := h.fetch(ctx)
	if err != nil {
		return fmt.Errorf("dydx: fetch block height: %w", err)
	}
	if height == 0 {
		return fmt.Errorf("dydx: venue reported block height 0")
	}
	observedAt := h.now()

	h.mu.Lock()
	defer h.mu.Unlock()
	// Heights only move forward. A lower reading means the node we reached is
	// behind (a load-balanced RPC can round-robin across nodes); keeping the
	// higher value would be the unsafe direction, so accept it but do not let
	// it look fresher than it is.
	if height < h.height {
		return fmt.Errorf("dydx: block height went backwards: %d after %d", height, h.height)
	}
	h.height, h.fetchedAt = height, observedAt
	return nil
}

// current returns the cached height, or an error when it was never fetched or
// has gone stale.
func (h *heightTracker) current() (uint32, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fetchedAt.IsZero() {
		return 0, fmt.Errorf("dydx: block height is unknown")
	}
	if age := h.now().Sub(h.fetchedAt); age > h.staleAfter {
		return 0, fmt.Errorf("dydx: block height is stale (last observed %s ago, limit %s)",
			age, h.staleAfter)
	}
	return h.height, nil
}

// goodTilBlock returns the expiry block for a short-term order submitted now.
func (h *heightTracker) goodTilBlock() (uint32, error) {
	height, err := h.current()
	if err != nil {
		return 0, err
	}
	return height + shortBlockForward, nil
}
