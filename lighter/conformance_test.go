package lighter

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/conformance"
)

// TestConformance runs the shared VenueExecutor contract against this adapter's
// fake venue. Clauses live in internal/conformance; what follows is only the
// wiring that lets them drive a Lighter — and, in Skips, what this venue makes
// undrivable.
func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) conformance.Harness {
		venue := newFakeVenue(t)
		executor, _, collector, closed := newTestExecutorStream(t, venue)

		var tradeID atomic.Int64
		tradeID.Store(104_838)

		return conformance.Harness{
			Executor: executor,
			Events:   collector,
			Closed:   closed,

			RestingOrder: testOrder(godex.IntentPostOnly),
			// A well-formed client order index, in the decimal shape the
			// adapter allocates, that no order was placed under.
			UntrackedOrderID: "999",

			Reconnect: func(t *testing.T) {
				t.Helper()
				mark := collector.Mark()
				if err := executor.ForceReconnect(); err != nil {
					t.Fatalf("ForceReconnect: %v", err)
				}
				// There is no catch-up to wait past: the account stream
				// carries no execution history and the adapter asks for none,
				// so the new connection being up is the whole of it.
				if _, err := collector.WaitFor(t.Context(), mark, testEventTimeout,
					"reconnected", isConnectedEvent); err != nil {
					t.Fatal(err)
				}
			},

			EndCanceled: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				venue.push(t, postOnlyCanceledFrame(t, id))
			},

			Fill: func(t *testing.T, id godex.OrderID) {
				t.Helper()
				venue.push(t, tradeFrame(t, id, tradeID.Add(1)))
			},

			Skips: map[string]string{
				// Both follow from the same venue limitation, recorded on the
				// lighter package comment: the account stream reports only the
				// post-only cancellation status, and there is no order-status
				// endpoint to ask instead.
				"cancel/an_order_that_filled_is_not_reported_as_canceled": "VENUE: the account " +
					"stream never reports an order as filled, so nothing retires one and a cancel " +
					"racing a fill cannot be staged",
				"reconnect/re_checks_orders_believed_live": "VENUE: there is no order-status query, " +
					"so an order that ended while the stream was down cannot be asked about",
			},
		}
	})
}

// tradeFrame is one account_all trade naming this account as the buyer, keyed
// to the client order index behind id — which is what attributes the execution
// to a caller's order.
func tradeFrame(t *testing.T, id godex.OrderID, tradeID int64) []byte {
	t.Helper()
	clientOrderIndex, err := strconv.ParseInt(string(id), 10, 64)
	if err != nil {
		t.Fatalf("order id %q is not a client order index: %v", id, err)
	}
	return fmt.Appendf(nil, `{"account":48,"channel":"account_all:48","type":"update/account_all",`+
		`"assets":null,"positions":{},"shares":[],"funding_histories":{},"trades":{"2":[{`+
		`"trade_id":%d,"trade_id_str":"%d","tx_hash":"0xabc","type":"trade","market_id":2,`+
		`"size":"0.200","price":"82.236","usd_amount":"16.447200",`+
		`"ask_id":844424930414531,"ask_id_str":"844424930414531",`+
		`"bid_id":1125899906550902,"bid_id_str":"1125899906550902",`+
		`"ask_client_id":178318057992411,"ask_client_id_str":"178318057992411",`+
		`"bid_client_id":%d,"bid_client_id_str":"%d",`+
		`"ask_account_id":7,"bid_account_id":48,"is_maker_ask":true,`+
		`"block_height":10667,"timestamp":1783182477422}]}}`,
		tradeID, tradeID, clientOrderIndex, clientOrderIndex)
}
