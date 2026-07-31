package hyperliquid

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

func TestConnectEmitsVerifiedSnapshot(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	metadata := mustConnect(t, executor)

	if got, want := metadata.SizeStep.String(), "0.0001"; got != want {
		t.Errorf("SizeStep = %s, want %s", got, want)
	}
	// ETH's schedule tightens from 25x to 5x above $50k of notional. The
	// contract carries one fraction, so it must be the strictest tier —
	// 1/(2·5) — not the headline 1/(2·25) that would overstate headroom.
	if got, want := metadata.MaintenanceMarginFraction.String(), "0.1000"; got != want {
		t.Errorf("MaintenanceMarginFraction = %s, want %s", got, want)
	}

	mark := 0
	connected, at, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "connected", isConnectedEvent)
	if err != nil {
		t.Fatalf("connected: %v", err)
	}
	if connected.(godex.ConnectedEvent).VenueID != godex.VenueHyperliquid {
		t.Errorf("connected event carries the wrong venue: %+v", connected)
	}
	position, at, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "position", isPositionEvent)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if got := position.(godex.PositionEvent).Position; got.Symbol != testSymbol || !got.Size.IsZero() {
		t.Errorf("expected a flat %s position, got %+v", testSymbol, got)
	}
	margin, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "margin", isMarginEvent)
	if err != nil {
		t.Fatalf("margin: %v", err)
	}
	if got, want := margin.(godex.MarginEvent).EquityUSD.String(), "1000.000000"; got != want {
		t.Errorf("equity = %s, want %s", got, want)
	}

	subscriptions := strings.Join(venue.subscriptions(), " ")
	for _, channel := range []string{channelUserFills, channelOrderUpdates} {
		if !strings.Contains(subscriptions, channel) {
			t.Errorf("missing subscription for %q in %q", channel, subscriptions)
		}
	}
	if !strings.Contains(subscriptions, strings.ToLower(testAccount)) {
		t.Errorf("subscriptions do not carry the account address: %q", subscriptions)
	}
}

func TestConnectPublishesOpenPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(string(loadFixture(t, "clearinghouse_long.json")))
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	event, _, err := collector.WaitForAt(t.Context(), 0, testEventTimeout, "position", isPositionEvent)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	position := event.(godex.PositionEvent).Position
	if got, want := position.Size.String(), "0.5000"; got != want {
		t.Errorf("size = %s, want %s", got, want)
	}
	if got, want := position.EntryPrice.String(), "2986.3"; got != want {
		t.Errorf("entry = %s, want %s", got, want)
	}
	if got, want := position.UnrealizedPnL.String(), "-0.0134"; got != want {
		t.Errorf("upnl = %s, want %s", got, want)
	}
}

func TestConnectRejectsUnsupportedPerps(t *testing.T) {
	tests := []struct {
		name string
		coin string
		want string
	}{
		{"unknown coin", "NOPE", "not found in universe"},
		{"delisted", "OLD", "delisted"},
		{"isolated only", "ISO", "isolated-margin only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			venue := newFakeVenue(t)
			executor, _ := newTestExecutor(t, venue)
			executor.cfg.coin = test.coin
			_, err := executor.Connect(t.Context())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Connect error = %v, want one mentioning %q", err, test.want)
			}
		})
	}
}

func TestConnectRejectsForeignNonZeroPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(`{
      "assetPositions":[{"position":{"coin":"BTC","szi":"0.5","entryPx":"60000","unrealizedPnl":"0",
        "leverage":{"type":"cross","value":10}},"type":"oneWay"}],
      "marginSummary":{"accountValue":"1000","totalMarginUsed":"0"},
      "withdrawable":"1000"}`)
	executor, _ := newTestExecutor(t, venue)
	_, err := executor.Connect(t.Context())
	if err == nil || !strings.Contains(err.Error(), "unsupported non-zero position on BTC") {
		t.Fatalf("Connect error = %v, want an unsupported-position error", err)
	}
}

func TestConnectRejectsIsolatedMarginPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(`{
      "assetPositions":[{"position":{"coin":"ETH","szi":"0.5","entryPx":"2986.3","unrealizedPnl":"0",
        "leverage":{"type":"isolated","value":20}},"type":"oneWay"}],
      "marginSummary":{"accountValue":"1000","totalMarginUsed":"0"},
      "withdrawable":"1000"}`)
	executor, _ := newTestExecutor(t, venue)
	_, err := executor.Connect(t.Context())
	if err == nil || !strings.Contains(err.Error(), "cross margin") {
		t.Fatalf("Connect error = %v, want a cross-margin error", err)
	}
}

// A position with size but no entry price is not a state the account can be
// in. Connect must re-read rather than publish it, and must fail if it never
// resolves.
func TestConnectRefusesSizeWithoutEntryPrice(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(`{
      "assetPositions":[{"position":{"coin":"ETH","szi":"0.5","entryPx":"0","unrealizedPnl":"0",
        "leverage":{"type":"cross","value":20}},"type":"oneWay"}],
      "marginSummary":{"accountValue":"1000","totalMarginUsed":"0"},
      "withdrawable":"1000"}`)
	executor, collector := newTestExecutor(t, venue)
	if _, err := executor.Connect(t.Context()); err == nil {
		t.Fatal("Connect succeeded on an unpriced position")
	}
	for _, event := range collector.Events() {
		if isPositionEvent(event) {
			t.Fatalf("an unpriced position was published: %+v", event)
		}
	}
}

func TestPlaceOrderPostOnlyRoundsAndSignsAsALO(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted || ack.VenueID != godex.VenueHyperliquid {
		t.Errorf("ack = %+v, want a submitted hyperliquid ack", ack)
	}
	if !strings.HasPrefix(string(ack.OrderID), "0x") || len(ack.OrderID) != 34 {
		t.Errorf("order id %q is not a 128-bit client order id", ack.OrderID)
	}

	order := venue.lastOrderWire(t)
	if order.Asset != testAssetIndex {
		t.Errorf("asset = %d, want %d", order.Asset, testAssetIndex)
	}
	if !order.IsBuy || order.ReduceOnly {
		t.Errorf("side flags = buy:%v reduceOnly:%v, want buy:true reduceOnly:false", order.IsBuy, order.ReduceOnly)
	}
	if order.OrderType.Limit.Tif != tifALO {
		t.Errorf("tif = %q, want %q", order.OrderType.Limit.Tif, tifALO)
	}
	// ETH carries szDecimals 4, so prices hold at most two decimals — and at
	// this magnitude the five-significant-figure rule is stricter still, at
	// one. A buy floors.
	if order.Price != "2986.3" {
		t.Errorf("price = %q, want %q", order.Price, "2986.3")
	}
	// 0.50005 floored to the 0.0001 step, rendered without trailing zeros.
	if order.Size != "0.5" {
		t.Errorf("size = %q, want %q", order.Size, "0.5")
	}
	if order.Cloid != string(ack.OrderID) {
		t.Errorf("cloid = %q, want the acked order id %q", order.Cloid, ack.OrderID)
	}
}

func TestPlaceOrderIOCUsesIocTif(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.5","avgPx":"2986.3","oid":77747314}}]}}}`,
	})

	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentIOC))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted {
		t.Errorf("ack status = %s, want %s", ack.Status, godex.AckSubmitted)
	}
	if got := venue.lastOrderWire(t).OrderType.Limit.Tif; got != tifIOC {
		t.Errorf("tif = %q, want %q", got, tifIOC)
	}
}

// A crossing post-only is a normal-path outcome: AckRejected plus an
// OrderRejectedEvent, never an error.
func TestPlaceOrderPostOnlyCrossIsAckRejected(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Post only order would have immediately matched, bbo was 2986.2"}]}}}`,
	})

	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder returned an error for a crossing post-only: %v", err)
	}
	if ack.Status != godex.AckRejected {
		t.Fatalf("ack status = %s, want %s", ack.Status, godex.AckRejected)
	}
	event, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	rejection := event.(godex.OrderRejectedEvent)
	if rejection.OrderID != ack.OrderID {
		t.Errorf("rejection order id = %s, want %s", rejection.OrderID, ack.OrderID)
	}
	if !strings.Contains(rejection.Reason, "Post only") {
		t.Errorf("rejection reason = %q, want the venue's text", rejection.Reason)
	}
}

// Every other venue refusal is an error, not a normal-path rejection: the
// contract reserves AckRejected for a crossing maker.
func TestPlaceOrderOtherRejectionIsError(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order must have minimum value of $10."}]}}}`,
	})

	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err == nil {
		t.Fatal("expected an error for a non-post-only rejection")
	}
	for _, event := range collector.Events()[mark:] {
		if isRejectionEvent(event) {
			t.Errorf("an error-path refusal emitted a normal-path rejection: %+v", event)
		}
	}
}

func TestPlaceOrderRejectsForeignSymbolAndIntent(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	foreign := testOrder(godex.IntentPostOnly)
	foreign.Symbol = "BTC-PERP"
	if _, err := executor.PlaceOrder(t.Context(), foreign); err == nil {
		t.Error("expected an error for a foreign symbol")
	}
	unsupported := testOrder("gtc")
	if _, err := executor.PlaceOrder(t.Context(), unsupported); err == nil {
		t.Error("expected an error for an unsupported intent")
	}
	if venue.exchangeCount() != 0 {
		t.Errorf("rejected orders reached the venue: %d submissions", venue.exchangeCount())
	}
}

func TestPlaceOrderNoncesIncreaseStrictly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	// A clock that does not advance still has to produce increasing nonces.
	frozen := time.Now()
	executor.cfg.now = func() time.Time { return frozen }

	for range 3 {
		if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err != nil {
			t.Fatalf("PlaceOrder: %v", err)
		}
	}
	nonces := venue.nonces()
	for i := 1; i < len(nonces); i++ {
		if nonces[i] <= nonces[i-1] {
			t.Fatalf("nonces are not strictly increasing: %v", nonces)
		}
	}
}

func TestCancelOrderRejectsUnknownID(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	err := executor.CancelOrder(t.Context(), "0x00000000000000000000000000000001")
	if !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("CancelOrder error = %v, want ErrUnknownOrder", err)
	}
}

func TestCancelOrderCancelsByClientOrderID(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}`,
	})

	if err := executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	cancel := venue.lastCancelWire(t)
	if cancel.Asset != testAssetIndex || cancel.Cloid != string(ack.OrderID) {
		t.Errorf("cancel = %+v, want asset %d cloid %s", cancel, testAssetIndex, ack.OrderID)
	}
	// The order is gone; a second cancel has nothing to address.
	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("second CancelOrder error = %v, want ErrUnknownOrder", err)
	}
}

// A cancel retried after an ambiguous first attempt must not fail just
// because the first one landed.
func TestCancelOrderIsIdempotentWhenAlreadyGone(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"error":"Order was never placed, already canceled, or filled."}]}}}`,
	})

	if err := executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder on an already-gone order = %v, want success", err)
	}
}

// An unknown outcome must halt further submissions rather than retry, then
// resolve by asking the venue what it actually holds.
func TestUnknownOutcomeLatchesFaultAndReconciles(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{delay: 400 * time.Millisecond, body: `{"status":"ok"}`})

	_, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
	submissions := venue.exchangeCount()
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("second PlaceOrder error = %v, want the latched fault", err)
	}
	if venue.exchangeCount() != submissions {
		t.Fatalf("a latched fault let another submission through: %d -> %d", submissions, venue.exchangeCount())
	}

	// The venue never took the order, so recovery drops it and resumes.
	venue.setOrderQueryStatus(queryStatusUnknownOid)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("fault never cleared: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	executor.stateMu.Lock()
	tracked := len(executor.orders)
	executor.stateMu.Unlock()
	if tracked != 1 {
		t.Errorf("tracked orders = %d, want only the order that succeeded after recovery", tracked)
	}
}

// A non-200 is not evidence that nothing was applied, so it must latch rather
// than be treated as a clean refusal.
func TestHTTPErrorLatchesFault(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{status: http.StatusTooManyRequests, body: `rate limited`})

	_, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
}

// A whole-request refusal ("status":"err") is a processed outcome: it must
// fail the call without latching a fault, because nothing was applied.
func TestWholeRequestRefusalDoesNotLatch(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{body: `{"status":"err","response":"Failed to deserialize the JSON body"}`})

	_, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err == nil || errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want a plain failure", err)
	}
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err != nil {
		t.Fatalf("a processed refusal blocked the next submission: %v", err)
	}
}

func TestUserFillsSnapshotSeedsWithoutPublishing(t *testing.T) {
	venue := newFakeVenue(t)
	// The opening snapshot is the account's history, not this executor's.
	venue.setSnapshotFills(991001)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()

	// A live fill after the snapshot proves the stream is being read at all,
	// so the absent snapshot fill is suppression rather than silence.
	venue.push(t, fillFrame(t, false, 991002))

	event, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatalf("fill: %v", err)
	}
	if got, want := event.(godex.FillEvent).Size.String(), "0.5"; got != want {
		t.Errorf("fill size = %s, want %s", got, want)
	}
	if fills := countFills(collector.Events()); fills != 1 {
		t.Errorf("published %d fills, want only the live one", fills)
	}
}

// Connect must not accept orders before the opening snapshot has been
// absorbed: a fill arriving in a late snapshot would be read as history and
// dropped.
func TestConnectWaitsForTheFillSnapshot(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setSuppressSnapshot(true)
	executor, _ := newTestExecutor(t, venue)

	_, err := executor.Connect(t.Context())
	if err == nil || !strings.Contains(err.Error(), "no userFills snapshot") {
		t.Fatalf("Connect error = %v, want a missing-snapshot failure", err)
	}
}

// The reconnect gate: a reconnect replays the snapshot, and nothing already
// reported may be published twice.
func TestReconnectDoesNotDuplicateFills(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setSnapshotFills(991001)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push(t, fillFrame(t, false, 991002))
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent); err != nil {
		t.Fatalf("fill: %v", err)
	}

	// The post-reconnect snapshot replays everything: one fill was already
	// reported, one was seen live, and one was missed while down.
	venue.setSnapshotFills(991001, 991002, 991003)
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	_, at, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "disconnected", isDisconnectedEvent)
	if err != nil {
		t.Fatalf("disconnected: %v", err)
	}
	_, at, err = collector.WaitForAt(t.Context(), at+1, testEventTimeout, "reconnected", isConnectedEvent)
	if err != nil {
		t.Fatalf("reconnected: %v", err)
	}
	if _, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "post-reconnect snapshot position", isPositionEvent); err != nil {
		t.Fatalf("post-reconnect position: %v", err)
	}

	fill, fillAt, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "missed fill", isFillEvent)
	if err != nil {
		t.Fatalf("missed fill: %v", err)
	}
	if got := fill.(godex.FillEvent); got.Size.String() != "0.5" {
		t.Errorf("unexpected replayed fill %+v", got)
	}
	// The replayed fill must land inside the new connection's window, not
	// before its ConnectedEvent.
	if fillAt <= at {
		t.Errorf("a replayed fill was published before the reconnect was announced")
	}
	if fills := countFills(collector.Events()[at:]); fills != 1 {
		t.Errorf("republished %d fills after reconnect, want only the missed one", fills)
	}
}

func TestOrderUpdateClosesOrderWithRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()

	venue.push(t, orderUpdateFrame(string(ack.OrderID), "canceled"))
	event, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	rejection := event.(godex.OrderRejectedEvent)
	if rejection.OrderID != ack.OrderID || rejection.Reason != "canceled" {
		t.Errorf("rejection = %+v, want the tracked order closed as canceled", rejection)
	}
	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("closed order is still tracked: %v", err)
	}
}

// A fully filled order is finished, but it is not a rejection: the contract
// reserves that event for orders that did not fill in full.
func TestOrderUpdateFilledEmitsNoRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()

	venue.push(t, orderUpdateFrame(string(ack.OrderID), "filled"))
	venue.push(t, fillFrame(t, false, 991004))
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent); err != nil {
		t.Fatalf("fill: %v", err)
	}
	for _, event := range collector.Events()[mark:] {
		if isRejectionEvent(event) {
			t.Errorf("a filled order produced a rejection: %+v", event)
		}
	}
}

// An unrecognized order status has no safe interpretation, so the connection
// is rebuilt rather than the status guessed at.
func TestUnknownOrderStatusAbortsConnection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()

	venue.push(t, orderUpdateFrame("0x0000000000000000000000000000abcd", "quantumCanceled"))
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "disconnected", isDisconnectedEvent); err != nil {
		t.Fatalf("expected the connection to abort: %v", err)
	}
}

func TestUnknownWebSocketChannelAbortsConnection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()

	venue.push(t, []byte(`{"channel":"candle","data":{}}`))
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "disconnected", isDisconnectedEvent); err != nil {
		t.Fatalf("expected the connection to abort: %v", err)
	}
}

func TestCloseIsTerminalAndIdempotent(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	if err := executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := executor.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrClosed) {
		t.Errorf("PlaceOrder after Close = %v, want ErrClosed", err)
	}
	if _, err := executor.Connect(t.Context()); !errors.Is(err, godex.ErrClosed) {
		t.Errorf("Connect after Close = %v, want ErrClosed", err)
	}
}

func TestReduceOnlySizeCeilsToCloseFully(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	order := testOrder(godex.IntentIOC)
	order.ReduceOnly = true
	// Dust below the step must still close the position, so reduce-only
	// ceils where a plain order would floor to zero and fail.
	order.Size = decimal.MustFromString("0.00005", 5)
	if _, err := executor.PlaceOrder(t.Context(), order); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	wire := venue.lastOrderWire(t)
	if !wire.ReduceOnly || wire.Size != "0.0001" {
		t.Errorf("reduce-only order = %+v, want reduceOnly with size 0.0001", wire)
	}
}

// --- frame builders ---

func fillFrame(t *testing.T, isSnapshot bool, tradeIDs ...int64) []byte {
	t.Helper()
	fills := make([]string, 0, len(tradeIDs))
	for _, tradeID := range tradeIDs {
		fills = append(fills, fmt.Sprintf(`{"coin":"ETH","px":"2986.3","sz":"0.5","side":"B",`+
			`"time":1753660000000,"oid":77738308,"tid":%d,"fee":"0.0447",`+
			`"cloid":"0x0000000000000000000000000000abcd"}`, tradeID))
	}
	return fmt.Appendf(nil, `{"channel":"userFills","data":{"isSnapshot":%t,"user":%q,"fills":[%s]}}`,
		isSnapshot, testAccount, strings.Join(fills, ","))
}

func orderUpdateFrame(cloid, status string) []byte {
	return fmt.Appendf(nil, `{"channel":"orderUpdates","data":[{"order":{"coin":"ETH","side":"B",`+
		`"limitPx":"2986.3","sz":"0.0","oid":77738308,"timestamp":1753660000000,"origSz":"0.5",`+
		`"cloid":%q},"status":%q,"statusTimestamp":1753660001000}]}`, cloid, status)
}

func countFills(events []godex.AccountEvent) int {
	count := 0
	for _, event := range events {
		if isFillEvent(event) {
			count++
		}
	}
	return count
}

// --- regressions for the reconciliation and lifecycle gaps ---

// An order left resting by an unknown outcome is one the caller cannot
// address: it never received the client order id. Reconciliation must remove
// it rather than clear the fault and leave it live.
func TestRecoveredLiveOrderIsCancelled(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	venue.queueExchange(scriptedExchange{delay: 400 * time.Millisecond, body: `{"status":"ok"}`})
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}

	// The venue turns out to be holding it, so recovery cancels it.
	venue.setOrderQuery(queryStatusOrder, orderStatusOpen)
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}`,
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("fault never cleared: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancelled := false
	for _, action := range venue.exchangeActionTypes(t) {
		if action == actionTypeCancelByCloid {
			cancelled = true
		}
	}
	if !cancelled {
		t.Error("the recovered order was never cancelled")
	}
	executor.stateMu.Lock()
	tracked := len(executor.orders)
	executor.stateMu.Unlock()
	if tracked != 1 {
		t.Errorf("tracked orders = %d, want only the order placed after recovery", tracked)
	}
}

// An "ok" envelope carrying a status the adapter cannot read may still have
// created a resting order, so it is an unknown outcome rather than a failure.
func TestUnreadableOrderStatusLatchesFault(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"order","data":{"statuses":[{"waitingForTrigger":{}}]}}}`,
	})

	_, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
}

func TestUnreadableCancelStatusLatchesFault(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.queueExchange(scriptedExchange{
		body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":["probably"]}}}`,
	})

	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("CancelOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
}

// orderUpdates is push-only and never replayed, so a cancellation that
// happened while the socket was down has to be discovered by asking.
func TestReconnectReconcilesTrackedOrders(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	// While the socket is down the venue drops the order; the update is lost.
	venue.setOrderQuery(queryStatusUnknownOid, "")
	mark := collector.Mark()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}

	event, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := event.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID {
		t.Errorf("rejection = %+v, want the tracked order closed", got)
	}
	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("reconciled order is still tracked: %v", err)
	}
}

// An order the venue reports as filled is finished, but closing it is not a
// rejection.
func TestReconnectReconciliationDoesNotRejectFilledOrders(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	venue.setOrderQuery(queryStatusOrder, orderStatusFilled)
	mark := collector.Mark()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout,
		"post-reconnect position", isPositionEvent); err != nil {
		t.Fatalf("post-reconnect position: %v", err)
	}
	for _, event := range collector.Events()[mark:] {
		if isRejectionEvent(event) {
			t.Errorf("a filled order was reported as rejected: %+v", event)
		}
	}
}

// A malformed fill aborts the connection. Its trade id must not be recorded,
// or the snapshot that follows the reconnect would skip it as already seen.
func TestMalformedFillIsNotMarkedSeen(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()

	venue.push(t, []byte(`{"channel":"userFills","data":{"isSnapshot":false,"fills":[`+
		`{"coin":"ETH","px":"not-a-number","sz":"0.5","side":"B","time":1753660000000,"oid":1,"tid":991009}]}}`))
	_, at, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "disconnected", isDisconnectedEvent)
	if err != nil {
		t.Fatalf("expected the connection to abort: %v", err)
	}

	executor.stateMu.Lock()
	_, seen := executor.fills.seen[991009]
	executor.stateMu.Unlock()
	if seen {
		t.Fatal("a fill that failed to normalize was recorded as already seen")
	}

	// The replay after reconnect must still publish it once it is readable.
	venue.setSnapshotFills(991009)
	_, at, err = collector.WaitForAt(t.Context(), at+1, testEventTimeout, "reconnected", isConnectedEvent)
	if err != nil {
		t.Fatalf("reconnected: %v", err)
	}
	if _, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "replayed fill", isFillEvent); err != nil {
		t.Fatalf("replayed fill: %v", err)
	}
}

// A flat account carries no position entry, so the margin mode is invisible in
// the clearinghouse snapshot. Orders do not carry one either, which is why it
// is checked before any is accepted.
func TestConnectRejectsIsolatedMarginWhenFlat(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setLeverageType("isolated")
	executor, _ := newTestExecutor(t, venue)

	_, err := executor.Connect(t.Context())
	if err == nil || !strings.Contains(err.Error(), "only cross is supported") {
		t.Fatalf("Connect error = %v, want a cross-margin error", err)
	}
	if venue.exchangeCount() != 0 {
		t.Error("an isolated-margin account was allowed to submit")
	}
}
