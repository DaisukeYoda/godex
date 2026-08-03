// Package conformance runs the shared godex.VenueExecutor contract — the
// clauses documented on godex.VenueExecutor and in events.go — over a venue
// adapter's own fake venue.
//
// Every adapter runs the same clauses under the same names, so the contract is
// written once rather than three times, and a fourth venue's adoption cost is
// fixed and known. A clause that does not run somewhere is declared in
// Harness.Skips with its reason, which turns "this venue does not report a
// caller's cancel" from an absence in a test file — indistinguishable from an
// oversight — into a printed, greppable fact.
//
// This suite covers the shared contract only. Venue-specific wire handling —
// the bulk of each adapter's own suite, and rightly venue-shaped — stays where
// it is.
package conformance

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/smoketest"
)

const (
	// eventTimeout bounds every wait for an event that must arrive. Clauses
	// drive a fake venue in-process, so this is a deadlock guard rather than a
	// latency budget.
	eventTimeout = 3 * time.Second
	// settleWindow is how long a clause waits on an event that must not
	// arrive. It is spent in full on every such clause, so it stays short.
	settleWindow = 200 * time.Millisecond
)

// Harness is the venue side of one clause run: a fresh, unconnected executor
// wired to a fake venue, plus the handful of things a clause needs to make that
// venue act.
//
// Every driver an adapter leaves nil must be named in Skips, or the clauses
// needing it fail rather than quietly passing over a venue that never ran them.
type Harness struct {
	// Executor is constructed but not yet connected: clauses that assert on
	// Connect need to observe it themselves.
	Executor godex.VenueExecutor

	// Events records the executor's whole account stream, from before Connect.
	Events *smoketest.Collector

	// Closed is closed once the account-event channel has closed and Events
	// holds the complete stream. Waiting on Close is not the same as waiting on
	// the channel it eventually closes.
	Closed <-chan struct{}

	// RestingOrder is a post-only order this venue accepts and leaves on the
	// book — the order the order-lifecycle clauses place.
	RestingOrder godex.NewOrder

	// UntrackedOrderID is an id well-formed for this venue that the executor
	// never issued.
	UntrackedOrderID godex.OrderID

	// Reconnect drops the account stream, lets the executor rebuild it, and
	// returns only once whatever catch-up follows the new connection has been
	// delivered — a fill backfill, an opening snapshot, an order reconciliation.
	// What that is, and so what proves it has landed, differs per venue, which
	// is why waiting for it is the adapter's job rather than a clause's.
	Reconnect func(t *testing.T)

	// EndCanceled has the venue report id finished without having filled. It
	// says how the order ended, not that a cancel was accepted — whether that
	// reads as godex.ReasonCanceledByRequest is the adapter's decision to make,
	// from whether the caller asked.
	EndCanceled func(t *testing.T, id godex.OrderID)

	// EndFilled has the venue report id finished by filling in full, the
	// execution included, so that a FillEvent for id observes it.
	EndFilled func(t *testing.T, id godex.OrderID)

	// Fill has the venue report one execution against id without ending the
	// order. Where the venue replays executions to a reconnecting client, the
	// fill joins whatever the catch-up reads — otherwise the clause about
	// re-reported fills would pass over a replay that never happened.
	Fill func(t *testing.T, id godex.OrderID)

	// ForgetOrder has the venue drop id and report nothing, as a cancellation
	// during an outage would.
	ForgetOrder func(t *testing.T, id godex.OrderID)

	// Skips names clauses that do not run here, mapped to why. Two things end
	// up in it and the reason says which, by opening with "VENUE:" for a
	// limitation no amount of adapter work would remove, or "GAP:" for
	// unfinished work in the adapter. Both are worth stating; conflating them
	// is how a gap comes to read as a law of nature.
	Skips map[string]string
}

// driver names one Harness field a clause needs, so a missing one can be
// reported by name.
type driver int

const (
	driverReconnect driver = iota
	driverEndCanceled
	driverEndFilled
	driverFill
	driverForgetOrder
)

func (d driver) String() string {
	switch d {
	case driverReconnect:
		return "Reconnect"
	case driverEndCanceled:
		return "EndCanceled"
	case driverEndFilled:
		return "EndFilled"
	case driverFill:
		return "Fill"
	case driverForgetOrder:
		return "ForgetOrder"
	default:
		return fmt.Sprintf("driver(%d)", int(d))
	}
}

func (h Harness) has(d driver) bool {
	switch d {
	case driverReconnect:
		return h.Reconnect != nil
	case driverEndCanceled:
		return h.EndCanceled != nil
	case driverEndFilled:
		return h.EndFilled != nil
	case driverFill:
		return h.Fill != nil
	case driverForgetOrder:
		return h.ForgetOrder != nil
	default:
		return false
	}
}

// clause is one contract statement, named for the sentence it enforces and
// carrying where that sentence is written down.
type clause struct {
	name  string
	cites string
	needs []driver
	run   func(t *testing.T, h Harness)
}

var clauses = []clause{
	{
		name:  "connect/emits_the_initial_snapshot",
		cites: "godex.go:85-88",
		run:   connectEmitsTheInitialSnapshot,
	},
	{
		name:  "close/emits_a_final_disconnect_then_closes_the_channel",
		cites: "godex.go:107-118",
		run:   closeEmitsAFinalDisconnectThenClosesTheChannel,
	},
	{
		name:  "close/is_terminal",
		cites: "godex.go:117-118",
		run:   closeIsTerminal,
	},
	{
		name:  "close/is_idempotent",
		cites: "godex.go:117-118",
		run:   closeIsIdempotent,
	},
	{
		name:  "cancel/an_untracked_id_is_an_error",
		cites: "godex.go:104-105",
		run:   cancelAnUntrackedIDIsAnError,
	},
	{
		name:  "cancel/is_reported_when_the_venue_says_the_order_ended",
		cites: "events.go:88-92",
		needs: []driver{driverEndCanceled},
		run:   cancelIsReportedWhenTheVenueSaysTheOrderEnded,
	},
	{
		name:  "cancel/an_order_that_filled_is_not_reported_as_canceled",
		cites: "events.go:88-92",
		needs: []driver{driverEndFilled},
		run:   cancelAnOrderThatFilledIsNotReportedAsCanceled,
	},
	{
		name:  "reject/is_reported_at_most_once",
		cites: "events.go:69-71",
		needs: []driver{driverEndCanceled},
		run:   rejectIsReportedAtMostOnce,
	},
	{
		name:  "reject/does_not_orphan_a_fill_that_follows_it",
		cites: "events.go:73-78",
		needs: []driver{driverEndCanceled, driverFill},
		run:   rejectDoesNotOrphanAFillThatFollowsIt,
	},
	{
		name:  "reconnect/connection_events_alternate_across_it",
		cites: "godex.go:76-80",
		needs: []driver{driverReconnect},
		run:   reconnectConnectionEventsAlternateAcrossIt,
	},
	{
		name:  "reconnect/re_checks_orders_believed_live",
		cites: "events.go:94-96",
		needs: []driver{driverForgetOrder, driverReconnect},
		run:   reconnectReChecksOrdersBelievedLive,
	},
	{
		name:  "reconnect/does_not_re_report_a_delivered_fill",
		cites: "godex.go:67-68",
		needs: []driver{driverFill, driverReconnect},
		run:   reconnectDoesNotReReportADeliveredFill,
	},
}

// Run runs every clause against the venue newHarness builds. Each clause gets
// its own harness, so no clause inherits another's venue state.
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()

	t.Run("harness/skips_name_real_clauses", func(t *testing.T) {
		checkSkips(t, newHarness(t).Skips)
	})

	for _, c := range clauses {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			if reason, skipped := h.Skips[c.name]; skipped {
				t.Skipf("%s (%s): %s", c.name, c.cites, reason)
			}
			for _, need := range c.needs {
				if !h.has(need) {
					t.Fatalf("the harness supplies no %s driver and does not skip %q: "+
						"either drive the clause or record in Skips why this venue cannot",
						need, c.name)
				}
			}
			c.run(t, h)
		})
	}
}

// checkSkips rejects a skip naming no clause. A skip is how a venue records
// that a clause does not apply to it, so a misspelled one silently stops
// excusing anything — and, worse, reads as if it still does.
func checkSkips(t *testing.T, skips map[string]string) {
	t.Helper()
	known := make(map[string]bool, len(clauses))
	for _, c := range clauses {
		known[c.name] = true
	}
	names := make([]string, 0, len(skips))
	for name := range skips {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !known[name] {
			t.Errorf("Skips names %q, which is not a clause", name)
		}
		reason := skips[name]
		if !strings.HasPrefix(reason, "VENUE:") && !strings.HasPrefix(reason, "GAP:") {
			t.Errorf("Skips[%q] = %q, which says neither VENUE: nor GAP: — a reader "+
				"cannot tell an unfixable limitation from unfinished work", name, reason)
		}
	}
}

// --- clauses ---

// Connect resolves the venue's metadata and emits a verified snapshot before
// returning, so a caller that has connected already knows where it stands.
func connectEmitsTheInitialSnapshot(t *testing.T, h Harness) {
	mustConnect(t, h)

	_, at, err := h.Events.WaitForAt(t.Context(), 0, eventTimeout, "connected", isConnected)
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot's two halves are reported independently; the contract fixes
	// only that both follow the ConnectedEvent.
	if _, err := h.Events.WaitFor(t.Context(), at+1, eventTimeout, "snapshot position", isPosition); err != nil {
		t.Error(err)
	}
	if _, err := h.Events.WaitFor(t.Context(), at+1, eventTimeout, "snapshot margin", isMargin); err != nil {
		t.Error(err)
	}
}

// Close emits the final DisconnectedEvent and only then closes the channel, so
// a consumer ranging over the stream sees the disconnect before the range ends
// and can treat that end as terminal.
func closeEmitsAFinalDisconnectThenClosesTheChannel(t *testing.T, h Harness) {
	mustConnect(t, h)
	if _, err := h.Events.WaitFor(t.Context(), 0, eventTimeout, "connected", isConnected); err != nil {
		t.Fatal(err)
	}

	if err := h.Executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitClosed(t, h)

	events := h.Events.Events()
	if len(events) == 0 {
		t.Fatal("the stream closed without ever emitting an event")
	}
	if err := smoketest.CheckClosedStream(events); err != nil {
		t.Errorf("AccountEvents contract: %v", err)
	}
	if last := events[len(events)-1]; !isDisconnected(last) {
		t.Errorf("last event before the channel closed is %s, want the final disconnect",
			smoketest.FormatEvent(last))
	}
}

// Close is terminal: reconnecting means constructing a new executor, so
// everything the closed one is asked to do afterwards refuses.
func closeIsTerminal(t *testing.T, h Harness) {
	mustConnect(t, h)
	if err := h.Executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := h.Executor.PlaceOrder(t.Context(), h.RestingOrder); !errors.Is(err, godex.ErrClosed) {
		t.Errorf("PlaceOrder after Close = %v, want ErrClosed", err)
	}
	if _, err := h.Executor.Connect(t.Context()); !errors.Is(err, godex.ErrClosed) {
		t.Errorf("Connect after Close = %v, want ErrClosed", err)
	}
}

// Closing twice is not an error. A shutdown path that cannot tell whether it
// already ran would otherwise have to guess.
func closeIsIdempotent(t *testing.T, h Harness) {
	mustConnect(t, h)
	if err := h.Executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.Executor.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// An id the executor is not tracking is not silently accepted: a caller that
// cancels the wrong order learns so.
func cancelAnUntrackedIDIsAnError(t *testing.T, h Harness) {
	mustConnect(t, h)
	if err := h.Executor.CancelOrder(t.Context(), h.UntrackedOrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("CancelOrder of an id never issued = %v, want ErrUnknownOrder", err)
	}
}

// A cancel is reported when the venue says the order ended, not when it accepts
// the request — and a caller's cancel reads the same on every venue.
func cancelIsReportedWhenTheVenueSaysTheOrderEnded(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	mark := h.Events.Mark()
	if err := h.Executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if count := rejectionsFor(h.Events.Events()[mark:], ack.OrderID); count != 0 {
		t.Errorf("accepting the cancel reported an outcome %d times before the venue said one", count)
	}

	h.EndCanceled(t, ack.OrderID)
	event, err := h.Events.WaitFor(t.Context(), mark, eventTimeout, "rejection", isRejectionFor(ack.OrderID))
	if err != nil {
		t.Fatal(err)
	}
	if reason := event.(godex.OrderRejectedEvent).Reason; reason != godex.ReasonCanceledByRequest {
		t.Errorf("reason = %q, want %q", reason, godex.ReasonCanceledByRequest)
	}
}

// A cancel accepted the instant the order filled applied to nothing. The order
// ended by executing, so it is reported as filled and never as cancelled.
func cancelAnOrderThatFilledIsNotReportedAsCanceled(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	mark := h.Events.Mark()
	if err := h.Executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}

	h.EndFilled(t, ack.OrderID)
	if _, err := h.Events.WaitFor(t.Context(), mark, eventTimeout, "fill", isFillFor(ack.OrderID)); err != nil {
		t.Fatal(err)
	}
	if count := rejectionsFor(h.Events.Events()[mark:], ack.OrderID); count != 0 {
		t.Errorf("a filled order was reported as cancelled %d times, want 0", count)
	}
}

// An order ends once, so it is reported finished once, however many times the
// venue says so. A consumer that books each rejection would otherwise
// double-count a re-sent report.
func rejectIsReportedAtMostOnce(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	mark := h.Events.Mark()
	h.EndCanceled(t, ack.OrderID)
	if _, err := h.Events.WaitFor(t.Context(), mark, eventTimeout, "rejection", isRejectionFor(ack.OrderID)); err != nil {
		t.Fatal(err)
	}

	// The venue repeats itself: a replayed frame, or a report that crossed a
	// reconnect. Nothing about the order changed.
	h.EndCanceled(t, ack.OrderID)
	settle(t, h)
	if count := rejectionsFor(h.Events.Events()[mark:], ack.OrderID); count != 1 {
		t.Errorf("the order was reported finished %d times, want 1", count)
	}
}

// A rejection can precede the fill it accounts for, when the venue reports the
// removal in an earlier message than the execution. Fills are attributed by
// order id, so the late one still lands on its order.
func rejectDoesNotOrphanAFillThatFollowsIt(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	mark := h.Events.Mark()
	h.EndCanceled(t, ack.OrderID)
	_, rejectedAt, err := h.Events.WaitForAt(t.Context(), mark, eventTimeout, "rejection",
		isRejectionFor(ack.OrderID))
	if err != nil {
		t.Fatal(err)
	}

	h.Fill(t, ack.OrderID)
	if _, err := h.Events.WaitFor(t.Context(), rejectedAt+1, eventTimeout,
		"fill after the rejection", isFillFor(ack.OrderID)); err != nil {
		t.Fatalf("%v — a rejection must not orphan a fill that follows it", err)
	}
}

// ConnectedEvent and DisconnectedEvent alternate across an internal reconnect
// too: the stream a consumer sees is one sequence of connected windows, not one
// per socket.
func reconnectConnectionEventsAlternateAcrossIt(t *testing.T, h Harness) {
	mustConnect(t, h)
	if _, err := h.Events.WaitFor(t.Context(), 0, eventTimeout, "connected", isConnected); err != nil {
		t.Fatal(err)
	}

	mark := h.Events.Mark()
	h.Reconnect(t)
	_, at, err := h.Events.WaitForAt(t.Context(), mark, eventTimeout, "disconnected", isDisconnected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Events.WaitFor(t.Context(), at+1, eventTimeout, "reconnected", isConnected); err != nil {
		t.Fatal(err)
	}
	if err := smoketest.CheckContract(h.Events.Events()); err != nil {
		t.Errorf("AccountEvents contract: %v", err)
	}
}

// An order can end while the stream is down, and a push-only order feed never
// says so. Each reconnect re-checks every order still believed live, so the
// outcome reaches the caller rather than the order resting in the executor's
// map forever.
func reconnectReChecksOrdersBelievedLive(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	// The venue drops the order while the socket is down; its update is lost.
	h.ForgetOrder(t, ack.OrderID)

	mark := h.Events.Mark()
	h.Reconnect(t)
	if _, err := h.Events.WaitFor(t.Context(), mark, eventTimeout, "rejection",
		isRejectionFor(ack.OrderID)); err != nil {
		t.Fatalf("%v — a reconnect must re-check orders believed live", err)
	}
	if err := h.Executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("the reconciled order is still tracked: CancelOrder = %v, want ErrUnknownOrder", err)
	}
}

// The account stream is the only source of truth for executions, so a
// reconnect's catch-up must not re-report one already delivered: a duplicate
// fill silently doubles the position a consumer believes it holds.
func reconnectDoesNotReReportADeliveredFill(t *testing.T, h Harness) {
	mustConnect(t, h)
	ack := mustPlace(t, h)

	mark := h.Events.Mark()
	h.Fill(t, ack.OrderID)
	if _, err := h.Events.WaitFor(t.Context(), mark, eventTimeout, "fill", isFillFor(ack.OrderID)); err != nil {
		t.Fatal(err)
	}

	// Reconnect returns once the new connection's catch-up has been delivered,
	// so a re-reported fill is already on the stream by the time it does.
	h.Reconnect(t)
	if count := fillsFor(h.Events.Events()[mark:], ack.OrderID); count != 1 {
		t.Errorf("one execution was reported %d times across the reconnect, want 1", count)
	}
}

// --- harness helpers ---

func mustConnect(t *testing.T, h Harness) {
	t.Helper()
	if _, err := h.Executor.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

func mustPlace(t *testing.T, h Harness) godex.OrderAck {
	t.Helper()
	ack, err := h.Executor.PlaceOrder(t.Context(), h.RestingOrder)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted {
		t.Fatalf("the harness's resting order was not accepted: ack = %+v", ack)
	}
	return ack
}

func waitClosed(t *testing.T, h Harness) {
	t.Helper()
	select {
	case <-h.Closed:
	case <-time.After(eventTimeout):
		t.Fatal("the account event channel never closed after Close")
	}
}

// settle gives an event a clause expects never to arrive the chance to arrive
// anyway, so that asserting on its absence is a real assertion rather than a
// race the adapter happens to win.
func settle(t *testing.T, h Harness) {
	t.Helper()
	_, _ = h.Events.WaitFor(t.Context(), len(h.Events.Events()), settleWindow, "a further event",
		func(godex.AccountEvent) bool { return true })
}

// --- event predicates ---

func isConnected(e godex.AccountEvent) bool    { _, ok := e.(godex.ConnectedEvent); return ok }
func isDisconnected(e godex.AccountEvent) bool { _, ok := e.(godex.DisconnectedEvent); return ok }
func isPosition(e godex.AccountEvent) bool     { _, ok := e.(godex.PositionEvent); return ok }
func isMargin(e godex.AccountEvent) bool       { _, ok := e.(godex.MarginEvent); return ok }

func isRejectionFor(id godex.OrderID) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == id
	}
}

func isFillFor(id godex.OrderID) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		fill, ok := e.(godex.FillEvent)
		return ok && fill.OrderID == id
	}
}

func rejectionsFor(events []godex.AccountEvent, id godex.OrderID) int {
	return count(events, isRejectionFor(id))
}

func fillsFor(events []godex.AccountEvent, id godex.OrderID) int {
	return count(events, isFillFor(id))
}

func count(events []godex.AccountEvent, predicate func(godex.AccountEvent) bool) int {
	n := 0
	for _, event := range events {
		if predicate(event) {
			n++
		}
	}
	return n
}
