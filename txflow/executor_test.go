package txflow

import (
	"errors"
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

	if got, want := metadata.SizeStep.String(), "0.001"; got != want {
		t.Errorf("SizeStep = %s, want %s", got, want)
	}
	// The venue exposes no maintenance schedule the adapter can read, so
	// the fraction is the one the caller supplied, unchanged.
	if got, want := metadata.MaintenanceMarginFraction.String(), testMarginFractionText; got != want {
		t.Errorf("MaintenanceMarginFraction = %s, want %s", got, want)
	}

	connected, at, err := collector.WaitForAt(t.Context(), 0, testEventTimeout, "connected", isConnectedEvent)
	if err != nil {
		t.Fatalf("connected: %v", err)
	}
	if connected.(godex.ConnectedEvent).VenueID != godex.VenueTxFlow {
		t.Errorf("connected event carries the wrong venue: %+v", connected)
	}
	position, at, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "position", isPositionEvent)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if got := position.(godex.PositionEvent).Position; got.Symbol != testSymbol || !got.Size.IsZero() || got.VenueID != godex.VenueTxFlow {
		t.Errorf("expected a flat %s position, got %+v", testSymbol, got)
	}
	margin, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "margin", isMarginEvent)
	if err != nil {
		t.Fatalf("margin: %v", err)
	}
	if got, want := margin.(godex.MarginEvent).EquityUSD.String(), "1000.000000"; got != want {
		t.Errorf("equity = %s, want %s", got, want)
	}

	// The subscription envelope names the channel beside the method as well
	// as inside the subscription, and carries the account.
	subscriptions := strings.Join(venue.subscriptions(), " ")
	if !strings.Contains(subscriptions, `"method":"subscribe"`) ||
		!strings.Contains(subscriptions, `"type":"orderUpdates"`) ||
		!strings.Contains(subscriptions, `"subscription":{"type":"orderUpdates","user":"`+testAccount+`"}`) {
		t.Errorf("unexpected subscription envelope: %q", subscriptions)
	}

	// Connect reads only allowlisted queries: perpMeta (not meta), the
	// margin mode, the fill history, and the account.
	requests := strings.Join(venue.infoRequests(), " ")
	for _, want := range []string{infoTypePerpMeta, infoTypeActiveAssetData, infoTypeUserFills, infoTypeClearinghouseState} {
		if !strings.Contains(requests, want) {
			t.Errorf("Connect did not query %q (queried: %s)", want, requests)
		}
	}
}

func TestConnectPublishesOpenPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(string(loadFixture(t, "clearinghouse_long.json")))
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	position, _, err := collector.WaitForAt(t.Context(), 0, testEventTimeout, "position", isPositionEvent)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	got := position.(godex.PositionEvent).Position
	if got.Size.String() != "0.5" || got.EntryPrice.String() != "2980.5" || got.UnrealizedPnL.String() != "2.9" {
		t.Errorf("position = %+v, want size 0.5 @ 2980.5 with pnl 2.9", got)
	}
	margin, _, err := collector.WaitForAt(t.Context(), 0, testEventTimeout, "margin", isMarginEvent)
	if err != nil {
		t.Fatalf("margin: %v", err)
	}
	// Usage is the share of equity that is not withdrawable:
	// (1002.9 - 853.875) / 1002.9.
	if got, want := margin.(godex.MarginEvent).UsageRatio.String(), "0.1486"; got != want {
		t.Errorf("usage = %s, want %s", got, want)
	}
}

func TestConnectRejectsUnsupportedPerps(t *testing.T) {
	for market, want := range map[string]string{
		"HALT-USDC": "trading halted",
		"ISO-USDC":  "isolated-margin only",
		"GONE-USDC": "delisted",
		"BAD-USDC":  "disagrees with szDecimals",
		"NOPE-USDC": "not found",
	} {
		t.Run(market, func(t *testing.T) {
			venue := newFakeVenue(t)
			cfg := testConfig(venue)
			cfg.Market = market
			executor, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = executor.Close() })
			_, err = executor.Connect(t.Context())
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Connect error = %v, want %q", err, want)
			}
		})
	}
}

func TestConnectRejectsForeignNonZeroPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(strings.Replace(string(loadFixture(t, "clearinghouse_long.json")),
		`"coin": "ETH-USDC"`, `"coin": "BTC-USDC"`, 1))
	executor, _ := newTestExecutor(t, venue)
	if _, err := executor.Connect(t.Context()); err == nil || !strings.Contains(err.Error(), "non-zero position on BTC-USDC") {
		t.Fatalf("Connect error = %v, want a foreign-position refusal", err)
	}
}

func TestConnectRejectsIsolatedMarginPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(strings.Replace(string(loadFixture(t, "clearinghouse_long.json")),
		`"type": "Cross"`, `"type": "Isolated"`, 1))
	executor, _ := newTestExecutor(t, venue)
	if _, err := executor.Connect(t.Context()); err == nil || !strings.Contains(err.Error(), "cross margin") {
		t.Fatalf("Connect error = %v, want a margin-mode refusal", err)
	}
}

func TestConnectRejectsIsolatedMarginWhenFlat(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setLeverageType("Isolated")
	executor, _ := newTestExecutor(t, venue)
	if _, err := executor.Connect(t.Context()); err == nil || !strings.Contains(err.Error(), "only cross is supported") {
		t.Fatalf("Connect error = %v, want a margin-mode refusal", err)
	}
}

func TestConnectRefusesSizeWithoutEntryPrice(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setClearinghouse(strings.Replace(string(loadFixture(t, "clearinghouse_long.json")),
		`"entryPx": "2980.5",`, ``, 1))
	executor, _ := newTestExecutor(t, venue)
	if _, err := executor.Connect(t.Context()); err == nil || !strings.Contains(err.Error(), "not fully applied") {
		t.Fatalf("Connect error = %v, want the snapshot to be refused", err)
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
	if ack.Status != godex.AckSubmitted || ack.VenueID != godex.VenueTxFlow || ack.OrderID == "" {
		t.Errorf("unexpected ack: %+v", ack)
	}
	wire := venue.lastOrderWire(t)
	// A buy rounds down to the market's 0.01 tick; size floors to the 0.001
	// step and is rendered without trailing zeros.
	if wire.Asset != testAssetIndex || !wire.IsBuy || wire.Price != "2986.35" || wire.Size != "0.5" ||
		wire.ReduceOnly || wire.OrderType.Limit.Tif != tifALO {
		t.Errorf("order wire = %+v", wire)
	}
	if got := executor.oidOf(t, ack.OrderID); got != firstFakeOid {
		t.Errorf("bound oid = %d, want %d", got, firstFakeOid)
	}
	// What was signed is the action itself, type and all fields included at
	// the JSON level.
	recording := executor.signer.(*recordingSigner)
	recording.mu.Lock()
	defer recording.mu.Unlock()
	if len(recording.actions) != 1 {
		t.Fatalf("signed %d actions, want 1", len(recording.actions))
	}
	action, ok := recording.actions[0].(orderAction)
	if !ok || action.Type != actionTypeOrder || action.Grouping != groupingNA || len(action.Orders) != 1 {
		t.Errorf("signed action = %#v", recording.actions[0])
	}
}

func TestPlaceOrderIOCUsesIocTif(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	order := testOrder(godex.IntentIOC)
	order.Side = godex.SideSell
	if _, err := executor.PlaceOrder(t.Context(), order); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	wire := venue.lastOrderWire(t)
	// A sell rounds up to the tick.
	if wire.IsBuy || wire.OrderType.Limit.Tif != tifIOC || wire.Price != "2986.36" {
		t.Errorf("order wire = %+v", wire)
	}
}

func TestPlaceOrderPostOnlyCrossIsAckRejected(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.queueExchange(scriptedExchange{body: `{"status":"ok","response":{"type":"order","data":{"statuses":[` +
		`{"error":"Post only order would have immediately matched, bbo was 2986.35@2986.36"}]}}}`})

	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckRejected {
		t.Errorf("ack status = %v, want AckRejected", ack.Status)
	}
	rejection, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || !strings.Contains(got.Reason, "Post only") {
		t.Errorf("rejection = %+v", got)
	}
	executor.stateMu.Lock()
	tracked := len(executor.orders)
	executor.stateMu.Unlock()
	if tracked != 0 {
		t.Errorf("a rejected order stayed tracked")
	}
}

func TestPlaceOrderOtherRejectionIsError(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{body: `{"status":"ok","response":{"type":"order","data":{"statuses":[` +
		`{"error":"Insufficient margin to place order."}]}}}`})
	_, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err == nil || !strings.Contains(err.Error(), "Insufficient margin") || errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want the venue's rejection", err)
	}
}

func TestPlaceOrderRejectsForeignSymbolAndIntent(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	foreign := testOrder(godex.IntentPostOnly)
	foreign.Symbol = "BTC-PERP"
	if _, err := executor.PlaceOrder(t.Context(), foreign); err == nil {
		t.Error("a foreign symbol was accepted")
	}
	odd := testOrder(godex.OrderIntent("gtc"))
	if _, err := executor.PlaceOrder(t.Context(), odd); err == nil {
		t.Error("an unsupported intent was accepted")
	}
	if venue.exchangeCount() != 0 {
		t.Error("a refused order reached the venue")
	}
}

func TestPlaceOrderNoncesIncreaseStrictly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	for i := 0; i < 5; i++ {
		if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err != nil {
			t.Fatalf("PlaceOrder %d: %v", i, err)
		}
	}
	nonces := venue.nonces()
	for i := 1; i < len(nonces); i++ {
		if nonces[i] <= nonces[i-1] {
			t.Fatalf("nonces not strictly increasing: %v", nonces)
		}
	}
}

func TestCancelOrderRejectsUnknownID(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	if err := executor.CancelOrder(t.Context(), "0x000000000000000000000000deadbeef"); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("CancelOrder error = %v, want ErrUnknownOrder", err)
	}
}

func TestCancelOrderCancelsByOid(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()
	if err := executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if got := venue.lastCancelWire(t); got.Asset != testAssetIndex || got.Oid != firstFakeOid {
		t.Errorf("cancel wire = %+v, want asset %d oid %d", got, testAssetIndex, firstFakeOid)
	}
	// An accepted cancel is not the order's end; the stream says when it
	// ended, and then it reads as the caller's cancel.
	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("second CancelOrder = %v, want ErrUnknownOrder", err)
	}
	venue.push(t, orderUpdateFrame(firstFakeOid, "Canceled"))
	rejection, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || got.Reason != godex.ReasonCanceledByRequest {
		t.Errorf("rejection = %+v", got)
	}
}

// "Never placed, already canceled, or filled" does not say which, so the venue's
// records are consulted and the order retired under what they say.
func TestCancelOrderOfAnAlreadyGoneOrderResolvesItsOutcome(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.setOrderState(t, firstFakeOid, "Canceled", false)
	venue.queueExchange(scriptedExchange{body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":[` +
		`{"error":"Order was never placed, already canceled, or filled."}]}}}`})
	mark := collector.Mark()
	if err := executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	rejection, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	// The caller's cancel did not end this order, so it does not read as
	// having done so.
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || got.Reason != "Canceled" {
		t.Errorf("rejection = %+v", got)
	}
}

func TestCancelOrderOfAnAlreadyGoneOrderThatFilledReportsNothing(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.setOrderState(t, firstFakeOid, orderStatusFilled, false)
	venue.queueExchange(scriptedExchange{body: `{"status":"ok","response":{"type":"cancel","data":{"statuses":[` +
		`{"error":"Order was never placed, already canceled, or filled."}]}}}`})
	if err := executor.CancelOrder(t.Context(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := countRejectionsFor(collector.Events(), ack.OrderID); n != 0 {
		t.Errorf("a filled order was reported rejected %d times", n)
	}
	executor.stateMu.Lock()
	_, tracked := executor.orders[ack.OrderID]
	executor.stateMu.Unlock()
	if tracked {
		t.Error("a filled order stayed tracked")
	}
}

// An outcome the adapter cannot read latches a fault; recovery finds the order
// the venue did take — by market and submission time, since no id came back —
// cancels it, and resumes.
func TestUnknownOutcomeRecoversTheOrderTheVenueTook(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	// The fake books the order before it stalls, which is exactly the case
	// a lost response leaves behind.
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

	waitForFaultToClear(t, executor)
	types := venue.exchangeActionTypes(t)
	// The stalled order was booked (fake oid firstFakeOid) but its response
	// carried nothing usable, so recovery must have cancelled it.
	if len(types) < 3 || types[1] != actionTypeCancel {
		t.Fatalf("exchange actions = %v, want the recovery cancel before trading resumed", types)
	}
	if got := venue.lastCancelWireAt(t, 1); got.Oid != firstFakeOid {
		t.Errorf("recovery cancelled oid %d, want %d", got.Oid, firstFakeOid)
	}
}

// When the venue's records hold nothing that could be the submission, it
// never landed: the order is dropped and trading resumes without a cancel.
func TestUnknownOutcomeWithNoVenueRecordClears(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{delay: 400 * time.Millisecond, lost: true})
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
	waitForFaultToClear(t, executor)
	for _, kind := range venue.exchangeActionTypes(t) {
		if kind == actionTypeCancel {
			t.Fatal("recovery cancelled an order the venue never held")
		}
	}
	executor.stateMu.Lock()
	tracked := len(executor.orders)
	executor.stateMu.Unlock()
	if tracked != 1 {
		t.Errorf("tracked orders = %d, want only the order that succeeded after recovery", tracked)
	}
}

// Several recent orders on the market that no tracked order accounts for
// cannot be told apart from the ambiguous submission, so nothing is claimed
// and submissions stay halted.
func TestUnknownOutcomeWithAmbiguousCandidatesStaysLatched(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	now := time.Now().UnixMilli()
	venue.addOrder(testMarket, orderStatusOpen, true, now)
	venue.addOrder(testMarket, orderStatusOpen, true, now)
	venue.queueExchange(scriptedExchange{delay: 400 * time.Millisecond, lost: true})
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want ErrTxOutcomeUnknown", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder error = %v, want the fault to stay latched", err)
	}
	if venue.exchangeCount() != 1 {
		t.Errorf("exchange calls = %d, want the halted executor to have sent nothing more", venue.exchangeCount())
	}
}

func waitForFaultToClear(t *testing.T, executor *Executor) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("fault never cleared: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A non-200 is not evidence that nothing was applied, so it must latch rather
// than be treated as a clean refusal.
func TestHTTPErrorLatchesFault(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	venue.queueExchange(scriptedExchange{status: http.StatusTooManyRequests, body: `rate limited`})
	if _, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
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

// The account's fill history is absorbed at Connect, and every later fill is
// published exactly once — attributed to the executor's order when the oid is
// one it bound, and unattributed otherwise.
func TestFillsArePolledAndAttributedByOid(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()

	venue.addFill(firstFakeOid, firstFakeTid)
	fill, at, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatalf("fill: %v", err)
	}
	if got := fill.(godex.FillEvent); got.OrderID != ack.OrderID || got.Price.String() != "2986.3" || got.Size.String() != "0.5" || got.Side != godex.SideBuy {
		t.Errorf("fill = %+v", got)
	}
	// A fill moved the position, so a fresh account read follows.
	if _, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "post-fill position", isPositionEvent); err != nil {
		t.Fatalf("post-fill position: %v", err)
	}

	// An execution by an order this executor did not place is still a
	// position change and is reported, with no order to attribute it to.
	venue.addFill(9999, firstFakeTid+1)
	foreign, _, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "foreign fill", isFillEvent)
	if err != nil {
		t.Fatalf("foreign fill: %v", err)
	}
	if got := foreign.(godex.FillEvent); got.OrderID != "" {
		t.Errorf("foreign fill was attributed to %q", got.OrderID)
	}
	// Fills on other markets are not this executor's business.
	venue.addFillOn("BTC-USDC", 9998, firstFakeTid+2)
	time.Sleep(100 * time.Millisecond)
	if fills := countFills(collector.Events()); fills != 2 {
		t.Errorf("published %d fills, want 2 (history and foreign markets excluded)", fills)
	}
}

func TestReconnectDoesNotDuplicateFills(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()
	venue.addFill(firstFakeOid, firstFakeTid)
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent); err != nil {
		t.Fatalf("fill: %v", err)
	}

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
	// A fill that happened while the socket was down is read on reconnect
	// and lands inside the new connection's window.
	venue.addFill(firstFakeOid, firstFakeTid+1)
	missed, missedAt, err := collector.WaitForAt(t.Context(), at+1, testEventTimeout, "missed fill", isFillEvent)
	if err != nil {
		t.Fatalf("missed fill: %v", err)
	}
	if got := missed.(godex.FillEvent); got.OrderID != ack.OrderID {
		t.Errorf("missed fill = %+v", got)
	}
	if missedAt <= at {
		t.Errorf("a fill was published before the reconnect was announced")
	}
	time.Sleep(100 * time.Millisecond)
	if fills := countFills(collector.Events()[at:]); fills != 1 {
		t.Errorf("published %d fills after reconnect, want only the missed one", fills)
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
	venue.push(t, orderUpdateFrame(firstFakeOid, "PartialCanceled"))
	rejection, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || got.Reason != "PartialCanceled" {
		t.Errorf("rejection = %+v", got)
	}
	if err := executor.CancelOrder(t.Context(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("CancelOrder after close = %v, want ErrUnknownOrder", err)
	}
}

func TestOrderUpdateFilledEmitsNoRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()
	venue.addFill(firstFakeOid, firstFakeTid)
	venue.push(t, orderUpdateFrame(firstFakeOid, orderStatusFilled))
	fill, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatalf("fill: %v", err)
	}
	// The order has ended, but its fill is still attributed to it.
	if got := fill.(godex.FillEvent); got.OrderID != ack.OrderID {
		t.Errorf("fill = %+v, want attribution to %s", got, ack.OrderID)
	}
	time.Sleep(100 * time.Millisecond)
	if n := countRejectionsFor(collector.Events(), ack.OrderID); n != 0 {
		t.Errorf("a filled order was reported rejected %d times", n)
	}
}

// The placing response and the order's first update race; an update that
// arrives first is held and applied the moment the oid is bound.
func TestOrderUpdateArrivingBeforeTheAckIsApplied(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	// The fake books the order (as firstFakeOid) before the scripted delay,
	// so the update can be pushed while the response is still pending.
	venue.queueExchange(scriptedExchange{delay: 100 * time.Millisecond})
	done := make(chan godex.OrderAck, 1)
	go func() {
		ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentIOC))
		if err != nil {
			t.Errorf("PlaceOrder: %v", err)
		}
		done <- ack
	}()
	time.Sleep(30 * time.Millisecond)
	venue.push(t, orderUpdateFrame(firstFakeOid, "Canceled"))
	ack := <-done
	rejection, _, err := collector.WaitForAt(t.Context(), 0, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || got.Reason != "Canceled" {
		t.Errorf("rejection = %+v", got)
	}
}

func TestUnknownOrderStatusAbortsConnection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.push(t, orderUpdateFrame(1, "QuantumCanceled"))
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

func TestPongAndSubscriptionAcksAreIgnored(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.push(t, []byte(`{"method":"PONG"}`))
	venue.push(t, []byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe"}}`))
	time.Sleep(100 * time.Millisecond)
	for _, event := range collector.Events()[mark:] {
		if isDisconnectedEvent(event) {
			t.Fatal("a keepalive or ack aborted the connection")
		}
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
	order.Size = decimal.MustFromString("0.0005", 4)
	if _, err := executor.PlaceOrder(t.Context(), order); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	wire := venue.lastOrderWire(t)
	if !wire.ReduceOnly || wire.Size != "0.001" {
		t.Errorf("reduce-only order = %+v, want reduceOnly with size 0.001", wire)
	}
}

// An order cancelled while the socket was down is never replayed; the reconnect
// asks the venue's records and reports what they say.
func TestReconnectReconcilesTrackedOrders(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.setOrderState(t, firstFakeOid, "Canceled", false)
	mark := collector.Mark()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	rejection, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "rejection", isRejectionEvent)
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if got := rejection.(godex.OrderRejectedEvent); got.OrderID != ack.OrderID || got.Reason != "Canceled" {
		t.Errorf("rejection = %+v", got)
	}
}

func TestReconnectReconciliationDoesNotRejectFilledOrders(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	ack, err := executor.PlaceOrder(t.Context(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	venue.setOrderState(t, firstFakeOid, orderStatusFilled, false)
	mark := collector.Mark()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "reconnected", isConnectedEvent); err != nil {
		t.Fatalf("reconnected: %v", err)
	}
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "post-reconnect position", isPositionEvent); err != nil {
		t.Fatalf("post-reconnect position: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := countRejectionsFor(collector.Events(), ack.OrderID); n != 0 {
		t.Errorf("a filled order was reported rejected %d times on reconnect", n)
	}
	executor.stateMu.Lock()
	_, tracked := executor.orders[ack.OrderID]
	executor.stateMu.Unlock()
	if tracked {
		t.Error("a filled order stayed tracked after reconciliation")
	}
}

// A fill that fails to normalize is not marked seen: the next poll gets
// another look at it once the payload is sane.
func TestMalformedFillIsNotMarkedSeen(t *testing.T) {
	venue := newFakeVenue(t)
	executor, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	mark := collector.Mark()
	venue.addFill(firstFakeOid, firstFakeTid)
	venue.setFillFields("not-a-price", "0.5")
	time.Sleep(100 * time.Millisecond)
	if fills := countFills(collector.Events()[mark:]); fills != 0 {
		t.Fatalf("a malformed fill was published")
	}
	venue.setFillFields("2986.3", "0.5")
	if _, _, err := collector.WaitForAt(t.Context(), mark, testEventTimeout, "fill", isFillEvent); err != nil {
		t.Fatalf("the repaired fill was never published: %v", err)
	}
}

func TestConfigResolution(t *testing.T) {
	venue := newFakeVenue(t)
	base := testConfig(venue)

	missingFraction := base
	missingFraction.MaintenanceMarginFraction = decimal.Decimal{}
	if _, err := New(missingFraction); err == nil || !strings.Contains(err.Error(), "MaintenanceMarginFraction") {
		t.Errorf("New without a margin fraction = %v, want a refusal", err)
	}

	// Testnet has no known endpoints or signing parameters: it resolves only
	// when every one of them is supplied.
	testnet := base
	testnet.Network = Testnet
	if _, err := New(testnet); err == nil || !strings.Contains(err.Error(), "testnet") {
		t.Errorf("New on testnet without overrides = %v, want a refusal", err)
	}
	testnet.Signing = SigningParams{Network: "TxFlow-Testnet", ChainID: 1, APIVersion: 1}
	if _, err := New(testnet); err != nil {
		t.Errorf("New on testnet with overrides: %v", err)
	}
	partial := base
	partial.Signing = SigningParams{Network: "TxFlow-Mainnet"}
	if _, err := New(partial); err == nil || !strings.Contains(err.Error(), "together") {
		t.Errorf("New with a partial signing override = %v, want a refusal", err)
	}

	mainnet := Config{
		Credentials:               base.Credentials,
		Symbol:                    testSymbol,
		Market:                    testMarket,
		Network:                   Mainnet,
		MaintenanceMarginFraction: base.MaintenanceMarginFraction,
	}
	executor, err := New(mainnet)
	if err != nil {
		t.Fatalf("New on mainnet: %v", err)
	}
	if executor.cfg.restBaseURL != mainnetRESTBaseURL || executor.cfg.wsURL != mainnetWSURL ||
		executor.cfg.signing != (SigningParams{Network: mainnetSigningNetwork, ChainID: mainnetSigningChainID, APIVersion: mainnetSigningAPIVersion}) {
		t.Errorf("mainnet resolved to %+v", executor.cfg)
	}
}
