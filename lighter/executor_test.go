package lighter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/smoketest"
	"github.com/elliottech/lighter-go/types/txtypes"
	"github.com/gorilla/websocket"
)

const (
	testEventTimeout = 3 * time.Second
	testAccountIndex = int64(48)
)

type scriptedSendTx struct {
	body  string
	delay time.Duration
}

// fakeVenue is an httptest-backed Lighter: REST endpoints from fixtures, a
// scriptable sendTx, and an account WS stream the tests can push frames into.
type fakeVenue struct {
	t      *testing.T
	server *httptest.Server
	wsURL  string

	mu           sync.Mutex
	nonce        int64
	nonceFetches int
	accountJSON  []byte
	sendTxQueue  []scriptedSendTx
	sendTxCalls  []url.Values
	conns        []*websocket.Conn
	inbound      []string
}

func newFakeVenue(t *testing.T) *fakeVenue {
	t.Helper()
	venue := &fakeVenue{t: t, nonce: 7, accountJSON: loadFixture(t, "account_rest.json")}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/orderBookDetails", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "order_book_details.json"))
	})
	mux.HandleFunc("/api/v1/nextNonce", func(w http.ResponseWriter, _ *http.Request) {
		venue.mu.Lock()
		venue.nonceFetches++
		nonce := venue.nonce
		venue.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"code":200,"nonce":%d}`, nonce)
	})
	mux.HandleFunc("/api/v1/account", func(w http.ResponseWriter, _ *http.Request) {
		venue.mu.Lock()
		account := venue.accountJSON
		venue.mu.Unlock()
		_, _ = w.Write(account)
	})
	mux.HandleFunc("/api/v1/sendTx", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("sendTx form parse: %v", err)
		}
		venue.mu.Lock()
		venue.sendTxCalls = append(venue.sendTxCalls, r.PostForm)
		script := scriptedSendTx{body: `{"code":200,"tx_hash":"0xabc"}`}
		if len(venue.sendTxQueue) > 0 {
			script = venue.sendTxQueue[0]
			venue.sendTxQueue = venue.sendTxQueue[1:]
		}
		venue.mu.Unlock()
		if script.delay > 0 {
			time.Sleep(script.delay)
		}
		_, _ = w.Write([]byte(script.body))
	})
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		venue.mu.Lock()
		venue.conns = append(venue.conns, conn)
		venue.mu.Unlock()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			venue.mu.Lock()
			venue.inbound = append(venue.inbound, string(data))
			venue.mu.Unlock()
		}
	})
	venue.server = httptest.NewServer(mux)
	venue.wsURL = "ws" + strings.TrimPrefix(venue.server.URL, "http") + "/stream"
	t.Cleanup(venue.server.Close)
	return venue
}

func (v *fakeVenue) queueSendTx(scripts ...scriptedSendTx) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sendTxQueue = append(v.sendTxQueue, scripts...)
}

func (v *fakeVenue) sendTxCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.sendTxCalls)
}

func (v *fakeVenue) push(t *testing.T, frame []byte) {
	t.Helper()
	v.mu.Lock()
	if len(v.conns) == 0 {
		v.mu.Unlock()
		t.Fatal("no ws connection to push into")
	}
	conn := v.conns[len(v.conns)-1]
	v.mu.Unlock()
	if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func (v *fakeVenue) setAccountJSON(body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.accountJSON = []byte(body)
}

type fakeSigner struct {
	mu          sync.Mutex
	createCalls []createOrderParams
	cancelCalls []int64 // client order indexes
}

func (f *fakeSigner) signCreateOrder(params createOrderParams) (uint8, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, params)
	return txtypes.TxTypeL2CreateOrder, fmt.Sprintf(`{"fake":"create","nonce":%d}`, params.nonce), nil
}

func (f *fakeSigner) signCancelOrder(_ int16, clientOrderIndex int64, nonce int64) (uint8, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, clientOrderIndex)
	return txtypes.TxTypeL2CancelOrder, fmt.Sprintf(`{"fake":"cancel","nonce":%d}`, nonce), nil
}

func (f *fakeSigner) createAuthToken(time.Time) (string, error) { return "fake-auth", nil }
func (f *fakeSigner) check() error                              { return nil }

func newTestExecutor(t *testing.T, venue *fakeVenue) (*Executor, *fakeSigner, *smoketest.Collector) {
	t.Helper()
	signerFake := &fakeSigner{}
	executor, err := New(Config{
		Credentials: Credentials{AccountIndex: testAccountIndex, APIKeyIndex: 2, APIPrivateKey: "ab"},
		Symbol:      "SOL-PERP",
		MarketID:    2,
		Network:     Testnet,
		Reconnect: godex.ReconnectConfig{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2,
			IdleTimeout:  time.Second,
		},
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		RESTBaseURL:          venue.server.URL,
		WSURL:                venue.wsURL,
		TxRequestTimeout:     250 * time.Millisecond,
		TxFaultRecoveryDelay: 100 * time.Millisecond,
		newSigner:            func(*resolvedConfig) (signer, error) { return signerFake, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	collector := smoketest.NewCollector(t.Logf)
	// Registered before the close cleanup so it runs after it (cleanups are
	// LIFO): the stream is complete, and its final DisconnectedEvent due, only
	// once Close has run and the channel has drained.
	t.Cleanup(func() {
		if err := smoketest.CheckClosedStream(collector.Events()); err != nil {
			t.Errorf("AccountEvents contract: %v", err)
		}
	})
	consumed := make(chan struct{})
	go func() {
		collector.Consume(executor.AccountEvents())
		close(consumed)
	}()
	t.Cleanup(func() {
		_ = executor.Close()
		<-consumed
	})
	return executor, signerFake, collector
}

func mustConnect(t *testing.T, executor *Executor) godex.ExecutionMetadata {
	t.Helper()
	metadata, err := executor.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return metadata
}

func testOrder(intent godex.OrderIntent) godex.NewOrder {
	return godex.NewOrder{
		Symbol: "SOL-PERP",
		Side:   godex.SideBuy,
		Price:  decimal.MustFromString("82.2356", 4),
		Size:   decimal.MustFromString("0.2001", 4),
		Intent: intent,
	}
}

func isPositionEvent(e godex.AccountEvent) bool { _, ok := e.(godex.PositionEvent); return ok }
func isMarginEvent(e godex.AccountEvent) bool   { _, ok := e.(godex.MarginEvent); return ok }
func isConnectedEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.ConnectedEvent)
	return ok
}
func isDisconnectedEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.DisconnectedEvent)
	return ok
}

func TestConnectEmitsVerifiedSnapshot(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	metadata := mustConnect(t, executor)

	if got := metadata.SizeStep.String(); got != "0.001" {
		t.Fatalf("sizeStep = %s", got)
	}
	// 240 in 1/10000ths normalizes to the plain ratio 0.0240.
	if got := metadata.MaintenanceMarginFraction.String(); got != "0.0240" {
		t.Fatalf("mmf = %s", got)
	}
	ctx := context.Background()
	if _, err := collector.WaitFor(ctx, 0, testEventTimeout, "connected", isConnectedEvent); err != nil {
		t.Fatal(err)
	}
	positionEvent, err := collector.WaitFor(ctx, 0, testEventTimeout, "position snapshot", isPositionEvent)
	if err != nil {
		t.Fatal(err)
	}
	if got := positionEvent.(godex.PositionEvent).Position.Size.String(); got != "-0.050" {
		t.Fatalf("position size = %s", got)
	}
	marginEvent, err := collector.WaitFor(ctx, 0, testEventTimeout, "margin snapshot", isMarginEvent)
	if err != nil {
		t.Fatal(err)
	}
	if got := marginEvent.(godex.MarginEvent).UsageRatio.String(); got != "0.1500" {
		t.Fatalf("usage = %s", got)
	}
}

func TestConnectFailsOnUnsupportedPosition(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setAccountJSON(`{"code":200,"accounts":[{"collateral":"100.0","available_balance":"85.0",
		"total_asset_value":"99.9","positions":[{"market_id":9,"sign":1,"position":"1.000",
		"avg_entry_price":"10.0","unrealized_pnl":"0","margin_mode":0}]}]}`)
	executor, _, _ := newTestExecutor(t, venue)
	_, err := executor.Connect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported non-zero position") {
		t.Fatalf("expected unsupported-position failure, got %v", err)
	}
}

func TestPlaceOrderPostOnly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, signerFake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted || ack.VenueID != godex.VenueLighter {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	signerFake.mu.Lock()
	params := signerFake.createCalls[0]
	signerFake.mu.Unlock()
	// Buy price 82.2356 floors to tick 0.001 -> 82.235 -> wire integer 82235;
	// size 0.2001 floors to step -> 0.200 -> wire integer 200.
	if params.price != 82235 || params.baseAmount != 200 {
		t.Fatalf("unexpected wire integers: %+v", params)
	}
	if params.marketIndex != 2 || params.isAsk || !params.postOnly || params.reduceOnly {
		t.Fatalf("unexpected params: %+v", params)
	}
	if params.orderExpiryAt == txtypes.NilOrderExpiry {
		t.Fatal("post-only must carry a GTT expiry")
	}
	if params.nonce != 7 {
		t.Fatalf("nonce = %d, want 7 (initial /nextNonce)", params.nonce)
	}
	if ack.OrderID != godex.OrderID(strconv.FormatInt(params.clientOrderIndex, 10)) {
		t.Fatalf("ack order id %s does not match client order index %d", ack.OrderID, params.clientOrderIndex)
	}
	venue.mu.Lock()
	call := venue.sendTxCalls[0]
	venue.mu.Unlock()
	if call.Get("tx_type") != strconv.Itoa(txtypes.TxTypeL2CreateOrder) {
		t.Fatalf("tx_type = %s", call.Get("tx_type"))
	}
	if !strings.Contains(call.Get("tx_info"), `"fake":"create"`) {
		t.Fatalf("tx_info = %s", call.Get("tx_info"))
	}
}

func TestPlaceOrderIOCBypassesMakerMinimums(t *testing.T) {
	venue := newFakeVenue(t)
	executor, signerFake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	order := testOrder(godex.IntentIOC)
	// Below min_base (0.050) and min_quote (10): takers only quantize to step.
	order.Size = decimal.MustFromString("0.010", 3)
	ack, err := executor.PlaceOrder(context.Background(), order)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	signerFake.mu.Lock()
	params := signerFake.createCalls[0]
	signerFake.mu.Unlock()
	if params.postOnly || params.orderExpiryAt != txtypes.NilOrderExpiry {
		t.Fatalf("IOC must be taker with nil expiry: %+v", params)
	}
	if params.baseAmount != 10 {
		t.Fatalf("baseAmount = %d, want 10", params.baseAmount)
	}
}

func TestPlaceOrderMakerBelowMinQuoteIsLocallyRejected(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	order := testOrder(godex.IntentPostOnly)
	order.Size = decimal.MustFromString("0.100", 3) // notional 8.2235 < min_quote 10
	ack, err := executor.PlaceOrder(context.Background(), order)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckRejected {
		t.Fatalf("expected local rejection, got %+v", ack)
	}
	rejectedEvent, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "order_rejected", func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == ack.OrderID
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rejectedEvent.(godex.OrderRejectedEvent).Reason, "min_quote_amount") {
		t.Fatalf("unexpected reason: %+v", rejectedEvent)
	}
	// The venue was never contacted.
	if venue.sendTxCount() != 0 {
		t.Fatalf("expected no sendTx calls, got %d", venue.sendTxCount())
	}
}

func TestPlaceOrderSynchronousPostOnlyRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	venue.queueSendTx(scriptedSendTx{body: `{"code":21120,"message":"post only order would have matched immediately"}`})
	mark := collector.Mark()
	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckRejected {
		t.Fatalf("expected rejected ack, got %+v", ack)
	}
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "order_rejected", func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == ack.OrderID
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPlaceOrderInvalidNonceResyncsAndRetriesOnce(t *testing.T) {
	venue := newFakeVenue(t)
	executor, signerFake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	venue.mu.Lock()
	venue.nonce = 30 // the server's real next nonce after drift
	venue.mu.Unlock()
	venue.queueSendTx(scriptedSendTx{body: `{"code":21505,"message":"invalid nonce"}`})

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted {
		t.Fatalf("expected submitted after retry, got %+v", ack)
	}
	if got := venue.sendTxCount(); got != 2 {
		t.Fatalf("sendTx calls = %d, want 2 (reject + retry)", got)
	}
	signerFake.mu.Lock()
	defer signerFake.mu.Unlock()
	if len(signerFake.createCalls) != 2 {
		t.Fatalf("sign calls = %d, want 2", len(signerFake.createCalls))
	}
	// The retry signs with the resynced server nonce.
	if signerFake.createCalls[1].nonce != 30 {
		t.Fatalf("retry nonce = %d, want 30", signerFake.createCalls[1].nonce)
	}
}

func TestPlaceOrderPersistentInvalidNonceFails(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	invalidNonce := scriptedSendTx{body: `{"code":21505,"message":"invalid nonce"}`}
	venue.queueSendTx(invalidNonce, invalidNonce)
	_, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err == nil || !strings.Contains(err.Error(), "nonce mismatch") {
		t.Fatalf("expected nonce-mismatch failure after the retry limit, got %v", err)
	}
	if got := venue.sendTxCount(); got != 2 {
		t.Fatalf("sendTx calls = %d, want 2 (retry limit 1)", got)
	}
}

func TestTxFaultLatchAndRecovery(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// The sendTx outcome is unknown: the handler outlives the 250ms client
	// timeout.
	venue.queueSendTx(scriptedSendTx{body: `{"code":200}`, delay: 600 * time.Millisecond})
	_, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("expected ErrTxOutcomeUnknown, got %v", err)
	}
	// The latch halts the next submission without touching the venue.
	beforeCount := venue.sendTxCount()
	_, err = executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("expected latched fault, got %v", err)
	}
	if venue.sendTxCount() != beforeCount {
		t.Fatal("latched fault must not reach the venue")
	}

	// Automatic recovery: nonce resync clears the latch and submissions
	// resume; the faulted transaction itself is never resent.
	deadline := time.Now().Add(3 * time.Second)
	for {
		ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
		if err == nil {
			if ack.Status != godex.AckSubmitted {
				t.Fatalf("unexpected ack after recovery: %+v", ack)
			}
			break
		}
		if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
			t.Fatalf("unexpected error while recovering: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("fault never recovered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := venue.sendTxCount(); got != beforeCount+1 {
		t.Fatalf("sendTx calls = %d, want %d (no resend of the faulted tx)", got, beforeCount+1)
	}
}

func TestCancelIsIdempotentlyRecoverableAfterTimeout(t *testing.T) {
	venue := newFakeVenue(t)
	executor, signerFake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil || ack.Status != godex.AckSubmitted {
		t.Fatalf("place: %v %+v", err, ack)
	}

	// First cancel times out ambiguously and latches the fault.
	venue.queueSendTx(scriptedSendTx{body: `{"code":200}`, delay: 600 * time.Millisecond})
	err = executor.CancelOrder(context.Background(), ack.OrderID)
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("expected ErrTxOutcomeUnknown, got %v", err)
	}

	// After recovery the same cancel succeeds: the order stays tracked and
	// cancel-by-client-index is idempotent venue-side.
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := executor.CancelOrder(context.Background(), ack.OrderID)
		if err == nil {
			break
		}
		if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
			t.Fatalf("unexpected error while recovering: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel never recovered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	signerFake.mu.Lock()
	defer signerFake.mu.Unlock()
	for _, clientOrderIndex := range signerFake.cancelCalls {
		if godex.OrderID(strconv.FormatInt(clientOrderIndex, 10)) != ack.OrderID {
			t.Fatalf("cancel signed for wrong order: %d", clientOrderIndex)
		}
	}
}

// Accepting a cancel transaction is not the order having ended by it: this
// venue accepts and then rejects asynchronously. So nothing is reported until
// the account stream says how the order actually ended — and when it does, a
// cancel the caller asked for reads under the shared reason.
func TestCancelOrderIsReportedWhenTheStreamConfirmsIt(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	mark := collector.Mark()
	if err := executor.CancelOrder(context.Background(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if count := countRejectionsFor(collector.Events()[mark:], ack.OrderID); count != 0 {
		t.Errorf("an accepted cancel reported an outcome %d times before the venue said one", count)
	}
	// The order is no longer addressable, even though it is still tracked.
	if err := executor.CancelOrder(context.Background(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Errorf("second CancelOrder = %v, want ErrUnknownOrder", err)
	}

	venue.push(t, postOnlyCanceledFrame(t, ack.OrderID))
	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "rejection",
		func(e godex.AccountEvent) bool {
			rejected, ok := e.(godex.OrderRejectedEvent)
			return ok && rejected.OrderID == ack.OrderID
		})
	if err != nil {
		t.Fatalf("rejection: %v", err)
	}
	if reason := event.(godex.OrderRejectedEvent).Reason; reason != godex.ReasonCanceledByRequest {
		t.Errorf("rejection reason = %q, want %q", reason, godex.ReasonCanceledByRequest)
	}
	if count := countRejectionsFor(collector.Events()[mark:], ack.OrderID); count != 1 {
		t.Errorf("order was reported finished %d times, want 1", count)
	}
	// A finished order stops being tracked, so the map does not grow for the
	// process's lifetime.
	executor.stateMu.Lock()
	_, still := executor.orders[ack.OrderID]
	executor.stateMu.Unlock()
	if still {
		t.Error("a finished order is still tracked")
	}
}

// postOnlyCanceledFrame is the account-stream frame reporting one order
// finished, keyed to an order this executor actually placed (the fixture
// carries a fixed client order index).
func postOnlyCanceledFrame(t *testing.T, id godex.OrderID) []byte {
	t.Helper()
	clientOrderIndex, err := strconv.ParseInt(string(id), 10, 64)
	if err != nil {
		t.Fatalf("order id %q is not a client order index: %v", id, err)
	}
	return fmt.Appendf(nil, `{"channel":"account_all_orders:48","type":"update/account_all_orders",`+
		`"orders":{"2":[{"order_index":1125899906551881,"client_order_index":%d,`+
		`"order_id":"1125899906551881","client_order_id":"%d","market_index":2,`+
		`"owner_account_index":48,"initial_base_amount":"0.200","price":"82.930",`+
		`"nonce":281474976419913,"remaining_base_amount":"0.000","is_ask":false,`+
		`"filled_base_amount":"0.000","filled_quote_amount":"0.000000","type":"limit",`+
		`"time_in_force":"post-only","reduce_only":false,"trigger_price":"0.000",`+
		`"order_expiry":1785601477198,"status":"canceled-post-only","block_height":10630,`+
		`"timestamp":1783182277}]}}`, clientOrderIndex, clientOrderIndex)
}

func countRejectionsFor(events []godex.AccountEvent, id godex.OrderID) int {
	count := 0
	for _, event := range events {
		if rejected, ok := event.(godex.OrderRejectedEvent); ok && rejected.OrderID == id {
			count++
		}
	}
	return count
}

func TestCancelUnknownOrder(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	if err := executor.CancelOrder(context.Background(), "999"); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("expected ErrUnknownOrder, got %v", err)
	}
}

func TestWsTradeEmitsFill(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push(t, loadFixture(t, "ws_account_update_with_trade.json"))
	fillEvent, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "fill", func(e godex.AccountEvent) bool {
		_, ok := e.(godex.FillEvent)
		return ok
	})
	if err != nil {
		t.Fatal(err)
	}
	fill := fillEvent.(godex.FillEvent)
	if fill.OrderID != "1783182472088" || fill.Side != godex.SideBuy {
		t.Fatalf("unexpected fill: %+v", fill)
	}
}

func TestWsPostOnlyCanceledEmitsRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push(t, loadFixture(t, "ws_orders_post_only_canceled.json"))
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "order_rejected", func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == "1783182277198" && rejected.Reason == orderStatusPostOnlyCanceled
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownWsMessageAbortsIntoReconnect(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push(t, []byte(`{"type":"update/pool_data","channel":"pool:1"}`))
	ctx := context.Background()
	// Fail fast: the unreliable connection is dropped, then automatically
	// rebuilt with fresh subscriptions.
	if _, err := collector.WaitFor(ctx, mark, testEventTimeout, "disconnected", isDisconnectedEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.WaitFor(ctx, mark, testEventTimeout, "reconnected", isConnectedEvent); err != nil {
		t.Fatal(err)
	}
}

func TestServerPingGetsPongReply(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	venue.push(t, []byte(`{"type":"ping"}`))
	deadline := time.Now().Add(testEventTimeout)
	for {
		venue.mu.Lock()
		var seen bool
		for _, message := range venue.inbound {
			if message == pongMessage {
				seen = true
			}
		}
		venue.mu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no pong reply observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStaleStateObservationsAreDropped(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// Reserve two sequences in order, then apply them out of order — the
	// late-arriving older observation must not overwrite newer state.
	executor.stateMu.Lock()
	older := executor.nextObservationSeqLocked()
	newer := executor.nextObservationSeqLocked()
	executor.stateMu.Unlock()

	newPosition := func(size string) godex.AccountEvent {
		return godex.PositionEvent{Position: godex.Position{
			VenueID: godex.VenueLighter, Symbol: "SOL-PERP",
			Size: decimal.MustFromString(size, 3), Time: time.Now(),
		}}
	}
	mark := collector.Mark()
	if applied := executor.applyStateEvents([]godex.AccountEvent{newPosition("0.100")}, newer); !applied {
		t.Fatal("newer observation must apply")
	}
	if applied := executor.applyStateEvents([]godex.AccountEvent{newPosition("0.999")}, older); applied {
		t.Fatal("stale observation must be dropped")
	}
	// Fills are never dropped, even at a stale sequence.
	staleFill := godex.FillEvent{OrderID: "1", Side: godex.SideBuy,
		Price: decimal.MustFromString("82.235", 3), Size: decimal.MustFromString("0.200", 3), Time: time.Now()}
	executor.applyStateEvents([]godex.AccountEvent{staleFill}, older)

	ctx := context.Background()
	if _, err := collector.WaitFor(ctx, mark, testEventTimeout, "fresh position", func(e godex.AccountEvent) bool {
		position, ok := e.(godex.PositionEvent)
		return ok && position.Position.Size.String() == "0.100"
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.WaitFor(ctx, mark, testEventTimeout, "stale-sequence fill", func(e godex.AccountEvent) bool {
		fill, ok := e.(godex.FillEvent)
		return ok && fill.OrderID == "1"
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range collector.Events()[mark:] {
		if position, ok := event.(godex.PositionEvent); ok && position.Position.Size.String() == "0.999" {
			t.Fatal("stale position leaked through")
		}
	}
}

func TestCloseEmitsDisconnectedThenClosesChannel(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	if _, err := collector.WaitFor(context.Background(), 0, testEventTimeout, "connected", isConnectedEvent); err != nil {
		t.Fatal(err)
	}

	mark := collector.Mark()
	if err := executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "final disconnected", isDisconnectedEvent); err != nil {
		t.Fatal(err)
	}
	// Terminal semantics after Close.
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentIOC)); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := executor.Connect(context.Background()); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := executor.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got %v", err)
	}
}

func TestPlaceOrderRejectsWrongSymbol(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)
	order := testOrder(godex.IntentIOC)
	order.Symbol = "BTC-PERP"
	if _, err := executor.PlaceOrder(context.Background(), order); err == nil || !strings.Contains(err.Error(), "configured for") {
		t.Fatalf("expected symbol mismatch error, got %v", err)
	}
}
