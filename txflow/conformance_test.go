package txflow

import (
	"sync/atomic"
	"testing"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/conformance"
)

// TestConformance runs the shared VenueExecutor contract against this adapter's
// fake venue. Clauses live in internal/conformance; what follows is only the
// wiring that lets them drive a TxFlow.
func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) conformance.Harness {
		venue := newFakeVenue(t)
		executor, collector, closed := newTestExecutorStream(t, venue)

		// Every fill needs its own trade id: the adapter dedupes on it, so a
		// reused one is dropped as already seen rather than reported.
		var tradeID atomic.Int64
		tradeID.Store(firstFakeTid)

		return conformance.Harness{
			Executor: executor,
			Events:   collector,
			Closed:   closed,

			RestingOrder: testOrder(godex.IntentPostOnly),
			// A well-formed id, in the 128-bit hex shape the adapter mints,
			// that no order was ever placed under.
			UntrackedOrderID: "0x000000000000000000000000deadbeef",

			Reconnect: func(t *testing.T) {
				t.Helper()
				mark := collector.Mark()
				if err := executor.ForceReconnect(); err != nil {
					t.Fatalf("ForceReconnect: %v", err)
				}
				// The reconnect re-reads fills and reconciles tracked orders,
				// then reads the account. That read landing means the rest
				// has been dispatched.
				if _, err := collector.WaitFor(t.Context(), mark, testEventTimeout,
					"post-reconnect position", isPositionEvent); err != nil {
					t.Fatal(err)
				}
			},

			EndCanceled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				oid := executor.oidOf(t, id)
				venue.setOrderState(t, oid, "Canceled", false)
				venue.push(t, orderUpdateFrame(oid, "Canceled"))
			},

			EndFilled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				oid := executor.oidOf(t, id)
				venue.setOrderState(t, oid, orderStatusFilled, false)
				venue.addFill(oid, tradeID.Add(1))
				venue.push(t, orderUpdateFrame(oid, orderStatusFilled))
			},

			Fill: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				// Fills are read by polling the account's history, so this
				// one is replayed on every later read — including the one a
				// reconnect makes, which is the path a duplicate would come
				// down.
				venue.addFill(executor.oidOf(t, id), tradeID.Add(1))
			},

			ForgetOrder: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				// orderUpdates is push-only, so an order dropped while the
				// socket is down is discoverable only by asking, and the venue
				// answers that it holds no such order.
				venue.forgetOrder(executor.oidOf(t, id))
			},
		}
	})
}
