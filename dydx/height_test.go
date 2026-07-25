package dydx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock so staleness can be tested without
// sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestHeightTrackerCurrentFailsBeforeFirstFetch(t *testing.T) {
	tracker := newHeightTracker(
		func(context.Context) (uint32, error) { return 100, nil },
		5*time.Second, newFakeClock().Now)

	if _, err := tracker.current(); err == nil {
		t.Fatal("expected an error before any height has been fetched")
	}
	if _, err := tracker.goodTilBlock(); err == nil {
		t.Fatal("goodTilBlock must not invent a height")
	}
}

func TestHeightTrackerRefreshThenCurrent(t *testing.T) {
	clock := newFakeClock()
	tracker := newHeightTracker(
		func(context.Context) (uint32, error) { return 1000, nil },
		5*time.Second, clock.Now)

	if err := tracker.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	height, err := tracker.current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if height != 1000 {
		t.Fatalf("height = %d, want 1000", height)
	}
	goodTilBlock, err := tracker.goodTilBlock()
	if err != nil {
		t.Fatalf("goodTilBlock: %v", err)
	}
	if goodTilBlock != 1000+shortBlockForward {
		t.Fatalf("goodTilBlock = %d, want %d", goodTilBlock, 1000+shortBlockForward)
	}
	// The order must stay inside the chain's acceptance window.
	if goodTilBlock <= height || goodTilBlock > height+shortBlockWindow+1 {
		t.Fatalf("goodTilBlock %d is outside the valid window (%d, %d]",
			goodTilBlock, height, height+shortBlockWindow+1)
	}
}

func TestHeightTrackerCurrentFailsWhenStale(t *testing.T) {
	clock := newFakeClock()
	tracker := newHeightTracker(
		func(context.Context) (uint32, error) { return 1000, nil },
		5*time.Second, clock.Now)

	if err := tracker.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	clock.advance(5 * time.Second)
	if _, err := tracker.current(); err != nil {
		t.Fatalf("a height exactly at the staleness limit must still be usable: %v", err)
	}
	clock.advance(time.Nanosecond)
	if _, err := tracker.current(); err == nil {
		t.Fatal("expected a stale-height error rather than an extrapolated height")
	}
}

func TestHeightTrackerRefreshFailureKeepsLastHeightUntilItGoesStale(t *testing.T) {
	clock := newFakeClock()
	var fail bool
	tracker := newHeightTracker(func(context.Context) (uint32, error) {
		if fail {
			return 0, errors.New("rpc unreachable")
		}
		return 1000, nil
	}, 5*time.Second, clock.Now)

	if err := tracker.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	fail = true
	clock.advance(time.Second)
	if err := tracker.refresh(context.Background()); err == nil {
		t.Fatal("expected the failing fetch to be reported")
	}
	// One failed poll is survivable: the cached height is still recent.
	if _, err := tracker.current(); err != nil {
		t.Fatalf("current: %v", err)
	}
	clock.advance(5 * time.Second)
	if _, err := tracker.current(); err == nil {
		t.Fatal("expected the height to go stale once refreshes keep failing")
	}
}

func TestHeightTrackerRejectsZeroAndBackwardsHeights(t *testing.T) {
	clock := newFakeClock()
	height := uint32(0)
	tracker := newHeightTracker(
		func(context.Context) (uint32, error) { return height, nil },
		5*time.Second, clock.Now)

	if err := tracker.refresh(context.Background()); err == nil {
		t.Fatal("expected height 0 to be rejected")
	}
	height = 1000
	if err := tracker.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	// A load-balanced RPC can answer from a lagging node; that reading must not
	// silently rewind the tracker.
	height = 990
	if err := tracker.refresh(context.Background()); err == nil {
		t.Fatal("expected a backwards height to be rejected")
	}
	current, err := tracker.current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if current != 1000 {
		t.Fatalf("height = %d, want the last good height 1000", current)
	}
}
