package dydx

import (
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/conformance"
)

// TestConformance runs the shared VenueExecutor contract against this adapter's
// fake venue. Clauses live in internal/conformance; what follows is only the
// wiring that lets them drive a dYdX.
func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) conformance.Harness {
		venue := newFakeVenue(t)
		executor, _, collector, closed := newTestExecutorStream(t, venue)

		// The Indexer keys fills by its own id, and the backfill after a
		// reconnect re-reads them all, so each one needs a distinct id.
		var fillID atomic.Int64

		// reportFill pushes one execution down the stream and adds it to the
		// Indexer's history, so a reconnect's backfill replays it exactly as
		// the real one would — which is the path a duplicate comes down.
		var history []map[string]any
		reportFill := func(t *testing.T, venueOrderID, size string) {
			t.Helper()
			id := "conformance-fill-" + strconv.FormatInt(fillID.Add(1), 10)
			venue.push(fillFrame(t, id, venueOrderID, "3010.2", size))
			history = append(history, fillObject(id, venueOrderID, "3010.2", size))
			body, err := json.Marshal(map[string]any{"fills": history})
			if err != nil {
				t.Fatalf("encode fill history: %v", err)
			}
			venue.setFills(string(body))
		}

		return conformance.Harness{
			Executor: executor,
			Events:   collector,
			Closed:   closed,

			RestingOrder: testOrder(godex.IntentPostOnly),
			// A well-formed client id, in the decimal shape the adapter
			// allocates, far past anything this executor will reach.
			UntrackedOrderID: "999999",

			Reconnect: func(t *testing.T) {
				t.Helper()
				mark := collector.Mark()
				if err := executor.ForceReconnect(); err != nil {
					t.Fatalf("ForceReconnect: %v", err)
				}
				// The subscription snapshot emits its backfilled fills before
				// the position they explain, so the position landing means the
				// catch-up is complete.
				if _, err := collector.WaitFor(t.Context(), mark, testEventTimeout,
					"post-reconnect position", isPositionEvent); err != nil {
					t.Fatal(err)
				}
			},

			EndCanceled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				clientID := clientIDFromOrderID(t, id)
				venue.push(orderFrame(t, clientID, venueOrderIDFor(clientID), orderStatusCanceled, nil))
			},

			EndFilled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				clientID := clientIDFromOrderID(t, id)
				venueOrderID := venueOrderIDFor(clientID)
				// The terminal update teaches the executor the venue's id for
				// this order; the mapping outlives the order, so the execution
				// that follows is still attributed to it.
				venue.push(orderFrame(t, clientID, venueOrderID, orderStatusFilled, nil))
				reportFill(t, venueOrderID, "0.5009")
			},

			Fill: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				clientID := clientIDFromOrderID(t, id)
				venueOrderID := venueOrderIDFor(clientID)
				venue.push(orderOpenFrame(t, clientID, venueOrderID))
				reportFill(t, venueOrderID, "0.100")
			},

			Skips: map[string]string{
				// A gap in this adapter, not a limitation of dYdX: the Indexer
				// has an open-orders endpoint and reconcileOrders already reads
				// it, but only while recovering an unknown submission outcome.
				// handleSubscribed indexes the snapshot's orders and does not
				// retire the ones missing from it, so an order cancelled while
				// the stream was down stays tracked and its OrderRejectedEvent
				// never arrives.
				"reconnect/re_checks_orders_believed_live": "GAP: the adapter reconciles tracked " +
					"orders only after an unknown submission outcome, not on reconnect",
			},
		}
	})
}

func clientIDFromOrderID(t *testing.T, id godex.OrderID) uint32 {
	t.Helper()
	clientID, err := strconv.ParseUint(string(id), 10, 32)
	if err != nil {
		t.Fatalf("order id %q is not a client id: %v", id, err)
	}
	return uint32(clientID)
}

func venueOrderIDFor(clientID uint32) string {
	return "venue-order-" + strconv.FormatUint(uint64(clientID), 10)
}
