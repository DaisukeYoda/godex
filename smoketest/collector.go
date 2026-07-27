package smoketest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
)

// Collector records account events and lets scenario code wait for specific
// ones. It is the harness-side view of the AccountEvents contract.
type Collector struct {
	logf func(format string, args ...any)

	mu      sync.Mutex
	events  []godex.AccountEvent
	updated chan struct{} // closed and replaced on every append
}

// NewCollector builds a Collector logging through logf.
func NewCollector(logf func(format string, args ...any)) *Collector {
	return &Collector{logf: logf, updated: make(chan struct{})}
}

// Consume drains ch into the collector until it closes.
func (c *Collector) Consume(ch <-chan godex.AccountEvent) {
	for event := range ch {
		c.push(event)
	}
}

func (c *Collector) push(event godex.AccountEvent) {
	c.mu.Lock()
	c.events = append(c.events, event)
	close(c.updated)
	c.updated = make(chan struct{})
	c.mu.Unlock()
	c.logf("event: %s", FormatEvent(event))
}

// Mark returns the index events recorded after this call will start at. Take
// it before triggering an action to wait only for its consequences.
func (c *Collector) Mark() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

// Events returns a snapshot of all recorded events.
func (c *Collector) Events() []godex.AccountEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	snapshot := make([]godex.AccountEvent, len(c.events))
	copy(snapshot, c.events)
	return snapshot
}

// WaitFor blocks until an event at or after fromIndex satisfies predicate.
func (c *Collector) WaitFor(ctx context.Context, fromIndex int, timeout time.Duration, label string, predicate func(godex.AccountEvent) bool) (godex.AccountEvent, error) {
	event, _, err := c.WaitForAt(ctx, fromIndex, timeout, label, predicate)
	return event, err
}

// WaitForAt is WaitFor, also reporting where the match landed. Checking an
// ordered sequence means continuing strictly after the previous match, so that
// an event which arrived before it cannot satisfy the next step.
func (c *Collector) WaitForAt(ctx context.Context, fromIndex int, timeout time.Duration, label string, predicate func(godex.AccountEvent) bool) (godex.AccountEvent, int, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	cursor := fromIndex
	for {
		c.mu.Lock()
		for ; cursor < len(c.events); cursor++ {
			if predicate(c.events[cursor]) {
				event, at := c.events[cursor], cursor
				c.mu.Unlock()
				return event, at, nil
			}
		}
		updated := c.updated
		c.mu.Unlock()
		select {
		case <-updated:
		case <-deadline.C:
			return nil, 0, fmt.Errorf("smoketest: timeout waiting for %s", label)
		case <-ctx.Done():
			return nil, 0, fmt.Errorf("smoketest: canceled waiting for %s: %w", label, ctx.Err())
		}
	}
}

// FormatEvent renders an event compactly for harness logs.
func FormatEvent(event godex.AccountEvent) string {
	switch e := event.(type) {
	case godex.FillEvent:
		return fmt.Sprintf("fill orderID=%s side=%s price=%s size=%s", e.OrderID, e.Side, e.Price, e.Size)
	case godex.PositionEvent:
		p := e.Position
		return fmt.Sprintf("position symbol=%s size=%s entry=%s upnl=%s", p.Symbol, p.Size, p.EntryPrice, p.UnrealizedPnL)
	case godex.MarginEvent:
		return fmt.Sprintf("margin usage=%s equityUSD=%s", e.UsageRatio, e.EquityUSD)
	case godex.OrderRejectedEvent:
		return fmt.Sprintf("order_rejected orderID=%s reason=%q", e.OrderID, e.Reason)
	case godex.ConnectedEvent:
		return fmt.Sprintf("connected venue=%s", e.VenueID)
	case godex.DisconnectedEvent:
		return fmt.Sprintf("disconnected venue=%s", e.VenueID)
	default:
		return fmt.Sprintf("unknown event %T", event)
	}
}
