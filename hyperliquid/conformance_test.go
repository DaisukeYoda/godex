package hyperliquid

import (
	"sync/atomic"
	"testing"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/conformance"
)

// TestConformance runs the shared VenueExecutor contract against this adapter's
// fake venue. Clauses live in internal/conformance; what follows is only the
// wiring that lets them drive a Hyperliquid.
func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) conformance.Harness {
		venue := newFakeVenue(t)
		executor, collector, closed := newTestExecutorStream(t, venue)

		// A resting order is one the venue still holds, so that is what it
		// answers until a clause says otherwise. The fake's own default is the
		// opposite, which would have every reconnect reconcile the order away
		// underneath clauses that are not about reconciliation at all.
		venue.setOrderQuery(queryStatusOrder, orderStatusOpen)

		// Every fill needs its own trade id: the adapter dedupes on it, so a
		// reused one is dropped as already seen rather than reported.
		var tradeID atomic.Int64
		tradeID.Store(880_000)

		return conformance.Harness{
			Executor: executor,
			Events:   collector,
			Closed:   closed,

			RestingOrder: testOrder(godex.IntentPostOnly),
			// A well-formed cloid, in the 128-bit hex shape the adapter mints,
			// that no order was ever placed under.
			UntrackedOrderID: "0x000000000000000000000000deadbeef",

			Reconnect: func(t *testing.T) {
				t.Helper()
				mark := collector.Mark()
				if err := executor.ForceReconnect(); err != nil {
					t.Fatalf("ForceReconnect: %v", err)
				}
				// The reconnect replays the userFills snapshot and reconciles
				// tracked orders, then reads the account. That read landing
				// means both are done.
				if _, err := collector.WaitFor(t.Context(), mark, testEventTimeout,
					"post-reconnect position", isPositionEvent); err != nil {
					t.Fatal(err)
				}
			},

			EndCanceled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				venue.push(t, orderUpdateFrame(string(id), "canceled"))
			},

			EndFilled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				venue.push(t, orderUpdateFrame(string(id), orderStatusFilled))
				venue.push(t, fillFrameFor(t, false, string(id), tradeID.Add(1)))
			},

			Fill: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				tid := tradeID.Add(1)
				// userFills opens every subscription with a snapshot, so the
				// reconnect after this replays the execution — which is the
				// path a duplicate would come down.
				venue.setSnapshotFillsFor(string(id), tid)
				venue.push(t, fillFrameFor(t, false, string(id), tid))
			},

			ForgetOrder: func(t *testing.T, _ godex.OrderID) {
				t.Helper()
				// orderUpdates is push-only, so an order dropped while the
				// socket is down is discoverable only by asking, and the venue
				// answers that it holds no such oid.
				venue.setOrderQuery(queryStatusUnknownOid, "")
			},
		}
	})
}
