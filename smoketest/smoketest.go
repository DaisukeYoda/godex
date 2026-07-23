// Package smoketest runs the venue-agnostic adoption-gate scenario against a
// live VenueExecutor (normally on testnet). A venue adapter is only adopted
// once every gate passes:
//
//  1. connect: metadata, connected event, and a verified account snapshot
//  2. far post-only order accepted, then canceled by executor-scoped ID
//  3. crossing post-only rejected as a normal-path outcome (never an error)
//  4. IOC executes: fill event then long position observed
//  5. optional reconnect check while holding the position: disconnect and
//     connect alternate, the snapshot re-converges, and no fill is duplicated
//  6. reduce-only IOC closes back to flat
//  7. optional natural maker fill near the touch
//
// Finally the whole event stream is checked against the AccountEvents
// ordering contract and fills are checked for duplicates.
package smoketest

import (
	"context"
	"fmt"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// Scenario price factors, applied at factorScale to the observed top of book.
const (
	// farPriceFactor keeps the resting maker order far from the touch.
	farPriceFactor = "0.90"
	// iocCapFactor crosses the book: as a post-only price it must be
	// rejected, as an IOC cap it fills immediately.
	iocCapFactor = "1.01"
	// closePriceFactor is the slippage cap for the reduce-only close.
	closePriceFactor = "0.99"
	factorScale      = 2
)

// Defaults for zero Config fields.
const (
	// DefaultEventTimeout bounds each single event wait.
	DefaultEventTimeout = 30 * time.Second
	// DefaultNaturalFillTimeout bounds the optional near-touch maker fill.
	DefaultNaturalFillTimeout = 10 * time.Minute
	// DefaultFarOrderRest lets the far maker order land on the book before
	// it is canceled.
	DefaultFarOrderRest = 3 * time.Second
)

// TOB is a top-of-book observation.
type TOB struct {
	BestBid decimal.Decimal
	BestAsk decimal.Decimal
}

// Config parameterizes the scenario.
type Config struct {
	Symbol godex.Symbol
	// Size is the order size for every scenario order. It must satisfy the
	// venue minimum notional even at the far price.
	Size decimal.Decimal
	// FetchTOB returns the venue's current top of book. Market data is
	// outside the executor contract, so the harness receives it injected.
	FetchTOB func(ctx context.Context) (TOB, error)
	// Logf receives progress and gate results.
	Logf func(format string, args ...any)
	// ForceReconnect, when non-nil, enables the reconnect gate: it must tear
	// down the account stream so the executor's automatic reconnect runs.
	ForceReconnect func() error
	// WaitFill enables the natural near-touch maker fill gate.
	WaitFill bool

	// EventTimeout, NaturalFillTimeout, and FarOrderRest override the
	// defaults when positive.
	EventTimeout       time.Duration
	NaturalFillTimeout time.Duration
	FarOrderRest       time.Duration
}

func (c *Config) validate() error {
	if c.Symbol == "" {
		return fmt.Errorf("smoketest: Symbol is required")
	}
	if c.Size.Sign() <= 0 {
		return fmt.Errorf("smoketest: Size must be positive")
	}
	if c.FetchTOB == nil {
		return fmt.Errorf("smoketest: FetchTOB is required")
	}
	if c.Logf == nil {
		return fmt.Errorf("smoketest: Logf is required")
	}
	if c.EventTimeout == 0 {
		c.EventTimeout = DefaultEventTimeout
	}
	if c.NaturalFillTimeout == 0 {
		c.NaturalFillTimeout = DefaultNaturalFillTimeout
	}
	if c.FarOrderRest == 0 {
		c.FarOrderRest = DefaultFarOrderRest
	}
	return nil
}

// Run executes the adoption-gate scenario. It owns the executor lifecycle:
// Connect at the start, Close at the end (also on failure). A non-nil error
// means at least one gate failed.
func Run(ctx context.Context, exec godex.VenueExecutor, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	collector := NewCollector(cfg.Logf)
	consumed := make(chan struct{})
	go func() {
		collector.Consume(exec.AccountEvents())
		close(consumed)
	}()
	defer func() {
		if err := exec.Close(); err != nil {
			cfg.Logf("executor close failed: %v", err)
		}
		// Close must close the event channel (contract); wait for the
		// consumer to drain so no logging outlives Run.
		<-consumed
	}()

	pass := func(gate string) { cfg.Logf("gate PASS: %s", gate) }

	// Gate 1: connect + verified snapshot.
	metadata, err := exec.Connect(ctx)
	if err != nil {
		return fmt.Errorf("smoketest: connect: %w", err)
	}
	cfg.Logf("metadata: sizeStep=%s mmf=%s", metadata.SizeStep, metadata.MaintenanceMarginFraction)
	if _, err := collector.WaitFor(ctx, 0, cfg.EventTimeout, "connected", isConnected); err != nil {
		return err
	}
	if _, err := collector.WaitFor(ctx, 0, cfg.EventTimeout, "position snapshot", isPosition); err != nil {
		return err
	}
	if _, err := collector.WaitFor(ctx, 0, cfg.EventTimeout, "margin snapshot", isMargin); err != nil {
		return err
	}
	pass("connect + snapshot")

	tob, err := cfg.FetchTOB(ctx)
	if err != nil {
		return fmt.Errorf("smoketest: fetch TOB: %w", err)
	}
	cfg.Logf("TOB: bid=%s ask=%s", tob.BestBid, tob.BestAsk)

	// Gate 2: far post-only accepted, then canceled.
	farAck, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol: cfg.Symbol,
		Side:   godex.SideBuy,
		Price:  scalePrice(tob.BestBid, farPriceFactor),
		Size:   cfg.Size,
		Intent: godex.IntentPostOnly,
	})
	if err != nil {
		return fmt.Errorf("smoketest: far post-only: %w", err)
	}
	if farAck.Status != godex.AckSubmitted {
		return fmt.Errorf("smoketest: far post-only should be submitted, got %s", farAck.Status)
	}
	select {
	case <-time.After(cfg.FarOrderRest):
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := exec.CancelOrder(ctx, farAck.OrderID); err != nil {
		return fmt.Errorf("smoketest: cancel far post-only: %w", err)
	}
	pass("far post-only + cancel")

	// Gate 3: crossing post-only rejected as a normal-path outcome. The
	// rejection may be synchronous (rejected ack) or asynchronous (a venue
	// accepts the tx and cancels via the account stream) — never an error.
	crossMark := collector.Mark()
	crossAck, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol: cfg.Symbol,
		Side:   godex.SideBuy,
		Price:  scalePrice(tob.BestAsk, iocCapFactor),
		Size:   cfg.Size,
		Intent: godex.IntentPostOnly,
	})
	if err != nil {
		return fmt.Errorf("smoketest: crossing post-only surfaced as an error (rejection pattern gap): %w", err)
	}
	if crossAck.Status != godex.AckRejected {
		if _, err := collector.WaitFor(ctx, crossMark, cfg.EventTimeout, "order_rejected via account stream",
			isOrderRejectedFor(crossAck.OrderID)); err != nil {
			return err
		}
	}
	pass("crossing post-only rejected (normal path)")

	// Gate 4: IOC executes — fill, then long position.
	iocMark := collector.Mark()
	iocAck, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol: cfg.Symbol,
		Side:   godex.SideBuy,
		Price:  scalePrice(tob.BestAsk, iocCapFactor),
		Size:   cfg.Size,
		Intent: godex.IntentIOC,
	})
	if err != nil {
		return fmt.Errorf("smoketest: ioc: %w", err)
	}
	if _, err := collector.WaitFor(ctx, iocMark, cfg.EventTimeout, "ioc fill", isFillFor(iocAck.OrderID)); err != nil {
		return err
	}
	longEvent, err := collector.WaitFor(ctx, iocMark, cfg.EventTimeout, "long position reflected", isPositionWithSign(1))
	if err != nil {
		return err
	}
	pass("ioc fill + long position")

	// Gate 5 (optional): reconnect while holding the position.
	if cfg.ForceReconnect != nil {
		if err := runReconnectGate(ctx, collector, cfg, longEvent.(godex.PositionEvent).Position); err != nil {
			return err
		}
		pass("reconnect: alternation + snapshot convergence")
	}

	// Gate 6: reduce-only IOC closes back to flat.
	closeTob, err := cfg.FetchTOB(ctx)
	if err != nil {
		return fmt.Errorf("smoketest: fetch TOB for close: %w", err)
	}
	closeMark := collector.Mark()
	if _, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol:     cfg.Symbol,
		Side:       godex.SideSell,
		Price:      scalePrice(closeTob.BestBid, closePriceFactor),
		Size:       cfg.Size,
		Intent:     godex.IntentIOC,
		ReduceOnly: true,
	}); err != nil {
		return fmt.Errorf("smoketest: reduce-only close: %w", err)
	}
	if _, err := collector.WaitFor(ctx, closeMark, cfg.EventTimeout, "flat position reflected", isPositionWithSign(0)); err != nil {
		return err
	}
	pass("reduce-only close to flat")

	// Gate 7 (optional): natural maker fill near the touch.
	if cfg.WaitFill {
		if err := runNaturalFillGate(ctx, exec, collector, cfg); err != nil {
			return err
		}
		pass("natural maker fill cycle")
	}

	// Stream-wide contract checks.
	events := collector.Events()
	if err := checkConnectionAlternation(events); err != nil {
		return err
	}
	pass("connected/disconnected alternation")
	if err := checkNoDuplicateFills(events); err != nil {
		return err
	}
	pass("no duplicate fills")

	cfg.Logf("event summary: %s", summarize(events))
	return nil
}

func runReconnectGate(ctx context.Context, collector *Collector, cfg Config, before godex.Position) error {
	mark := collector.Mark()
	if err := cfg.ForceReconnect(); err != nil {
		return fmt.Errorf("smoketest: force reconnect: %w", err)
	}
	if _, err := collector.WaitFor(ctx, mark, cfg.EventTimeout, "disconnected after forced reconnect", isDisconnected); err != nil {
		return err
	}
	if _, err := collector.WaitFor(ctx, mark, cfg.EventTimeout, "reconnected", isConnected); err != nil {
		return err
	}
	positionEvent, err := collector.WaitFor(ctx, mark, cfg.EventTimeout, "post-reconnect position snapshot", isPosition)
	if err != nil {
		return err
	}
	if _, err := collector.WaitFor(ctx, mark, cfg.EventTimeout, "post-reconnect margin snapshot", isMargin); err != nil {
		return err
	}
	after := positionEvent.(godex.PositionEvent).Position
	if after.Size.Cmp(before.Size) != 0 {
		return fmt.Errorf("smoketest: post-reconnect position diverged: before=%s after=%s", before.Size, after.Size)
	}
	return nil
}

func runNaturalFillGate(ctx context.Context, exec godex.VenueExecutor, collector *Collector, cfg Config) error {
	tob, err := cfg.FetchTOB(ctx)
	if err != nil {
		return fmt.Errorf("smoketest: fetch TOB for maker: %w", err)
	}
	mark := collector.Mark()
	makerAck, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol: cfg.Symbol,
		Side:   godex.SideBuy,
		Price:  tob.BestBid,
		Size:   cfg.Size,
		Intent: godex.IntentPostOnly,
	})
	if err != nil {
		return fmt.Errorf("smoketest: near-touch maker: %w", err)
	}
	cfg.Logf("waiting for natural maker fill (up to %s)", cfg.NaturalFillTimeout)
	if _, err := collector.WaitFor(ctx, mark, cfg.NaturalFillTimeout, "natural maker fill", isFillFor(makerAck.OrderID)); err != nil {
		return err
	}
	if _, err := collector.WaitFor(ctx, mark, cfg.EventTimeout, "maker position reflected", isPositionWithSign(1)); err != nil {
		return err
	}

	closeTob, err := cfg.FetchTOB(ctx)
	if err != nil {
		return fmt.Errorf("smoketest: fetch TOB after maker: %w", err)
	}
	closeMark := collector.Mark()
	if _, err := exec.PlaceOrder(ctx, godex.NewOrder{
		Symbol:     cfg.Symbol,
		Side:       godex.SideSell,
		Price:      scalePrice(closeTob.BestBid, closePriceFactor),
		Size:       cfg.Size,
		Intent:     godex.IntentIOC,
		ReduceOnly: true,
	}); err != nil {
		return fmt.Errorf("smoketest: close after maker: %w", err)
	}
	if _, err := collector.WaitFor(ctx, closeMark, cfg.EventTimeout, "flat after maker cycle", isPositionWithSign(0)); err != nil {
		return err
	}
	return nil
}

// scalePrice multiplies price by a factor string, keeping price's scale.
func scalePrice(price decimal.Decimal, factor string) decimal.Decimal {
	return price.MulToScale(decimal.MustFromString(factor, factorScale), price.Scale())
}

// Event predicates.
func isConnected(e godex.AccountEvent) bool    { _, ok := e.(godex.ConnectedEvent); return ok }
func isDisconnected(e godex.AccountEvent) bool { _, ok := e.(godex.DisconnectedEvent); return ok }
func isPosition(e godex.AccountEvent) bool     { _, ok := e.(godex.PositionEvent); return ok }
func isMargin(e godex.AccountEvent) bool       { _, ok := e.(godex.MarginEvent); return ok }

func isFillFor(id godex.OrderID) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		fill, ok := e.(godex.FillEvent)
		return ok && fill.OrderID == id
	}
}

func isOrderRejectedFor(id godex.OrderID) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == id
	}
}

func isPositionWithSign(sign int) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		position, ok := e.(godex.PositionEvent)
		return ok && position.Position.Size.Sign() == sign
	}
}

// checkConnectionAlternation verifies the AccountEvents ordering contract:
// connected/disconnected alternate and every other event sits inside a
// connected window.
func checkConnectionAlternation(events []godex.AccountEvent) error {
	connected := false
	for i, event := range events {
		switch event.(type) {
		case godex.ConnectedEvent:
			if connected {
				return fmt.Errorf("smoketest: event %d: connected while already connected", i)
			}
			connected = true
		case godex.DisconnectedEvent:
			if !connected {
				return fmt.Errorf("smoketest: event %d: disconnected while not connected", i)
			}
			connected = false
		default:
			if !connected {
				return fmt.Errorf("smoketest: event %d (%s) emitted outside a connected window", i, FormatEvent(event))
			}
		}
	}
	return nil
}

// checkNoDuplicateFills verifies no fill was delivered twice (the reconnect
// path must not re-emit executions).
func checkNoDuplicateFills(events []godex.AccountEvent) error {
	seen := make(map[string]int)
	for i, event := range events {
		fill, ok := event.(godex.FillEvent)
		if !ok {
			continue
		}
		key := fmt.Sprintf("%s|%d|%s|%s|%s", fill.OrderID, fill.Time.UnixNano(), fill.Side, fill.Price, fill.Size)
		if first, dup := seen[key]; dup {
			return fmt.Errorf("smoketest: duplicate fill (events %d and %d): %s", first, i, FormatEvent(fill))
		}
		seen[key] = i
	}
	return nil
}

func summarize(events []godex.AccountEvent) string {
	counts := make(map[string]int)
	for _, event := range events {
		counts[fmt.Sprintf("%T", event)]++
	}
	return fmt.Sprintf("%v", counts)
}
