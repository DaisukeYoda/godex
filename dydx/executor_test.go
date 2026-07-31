package dydx

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	authpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/auth/v1beta1"
	"github.com/DaisukeYoda/godex/smoketest"
)

const (
	testEventTimeout = 3 * time.Second
	testSymbol       = godex.Symbol("ETH-PERP")
	testTicker       = "ETH-USD"
	testAccountNum   = uint64(4242)
	testSequence     = uint64(7)
	testHeight       = uint32(38102400)
)

// scriptedBroadcast is one queued broadcast_tx_sync answer.
type scriptedBroadcast struct {
	body  string
	delay time.Duration
}

const acceptedBroadcast = `{"jsonrpc":"2.0","id":-1,"result":{"code":0,"log":"","hash":"ABC123"}}`

// fakeVenue is an httptest-backed dYdX: Indexer REST from fixtures, a CometBFT
// JSON-RPC with scriptable height and broadcast answers, and an account stream
// tests can push frames into.
type fakeVenue struct {
	t      *testing.T
	server *httptest.Server
	wsURL  string

	mu      sync.Mutex
	writeMu sync.Mutex // gorilla allows one concurrent writer per connection

	height          uint32
	heightErr       bool
	marketsJSON     []byte
	marketsGate     chan struct{} // when set, holds the markets response back
	subaccountJSON  []byte
	subaccountQueue []string      // served one per read, ahead of subaccountJSON
	subaccountGate  chan struct{} // when set, holds the subaccount response back
	subaccountReads int
	fillsJSON       []byte
	fillHistory     []map[string]any // newest first; enables real pagination
	fillPageQueries []string
	ordersJSON      string
	broadcastQueue  []scriptedBroadcast
	broadcastTxs    []string
	subscribeFrames []string
	conns           []*websocket.Conn
}

func newFakeVenue(t *testing.T) *fakeVenue {
	t.Helper()
	venue := &fakeVenue{
		t:              t,
		height:         testHeight,
		marketsJSON:    loadFixture(t, "perpetual_markets.json"),
		subaccountJSON: loadFixture(t, "subaccount_rest.json"),
		fillsJSON:      []byte(`{"fills":[]}`),
		ordersJSON:     `[]`,
	}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()

	mux.HandleFunc("/v4/perpetualMarkets", func(w http.ResponseWriter, r *http.Request) {
		venue.mu.Lock()
		body, gate := venue.marketsJSON, venue.marketsGate
		venue.mu.Unlock()
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v4/addresses/", func(w http.ResponseWriter, r *http.Request) {
		venue.mu.Lock()
		venue.subaccountReads++
		body, gate := venue.subaccountJSON, venue.subaccountGate
		if len(venue.subaccountQueue) > 0 {
			body, venue.subaccountQueue = []byte(venue.subaccountQueue[0]), venue.subaccountQueue[1:]
		}
		venue.mu.Unlock()
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v4/fills", func(w http.ResponseWriter, r *http.Request) {
		venue.mu.Lock()
		body, history := venue.fillsJSON, venue.fillHistory
		venue.mu.Unlock()
		if history == nil {
			_, _ = w.Write(body)
			return
		}
		_, _ = w.Write(venue.fillPage(t, history, r.URL.Query()))
	})
	mux.HandleFunc("/v4/orders", func(w http.ResponseWriter, _ *http.Request) {
		venue.mu.Lock()
		body := venue.ordersJSON
		venue.mu.Unlock()
		_, _ = io.WriteString(w, body)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		venue.mu.Lock()
		height, failing := venue.height, venue.heightErr
		venue.mu.Unlock()
		if failing {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintf(w,
			`{"result":{"sync_info":{"latest_block_height":"%d","catching_up":false}}}`, height)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("rpc request decode: %v", err)
			return
		}
		switch request.Method {
		case "abci_query":
			_, _ = io.WriteString(w, venue.accountQueryResponse(t))
		case "broadcast_tx_sync":
			var params struct {
				Tx string `json:"tx"`
			}
			_ = json.Unmarshal(request.Params, &params)
			venue.mu.Lock()
			venue.broadcastTxs = append(venue.broadcastTxs, params.Tx)
			script := scriptedBroadcast{body: acceptedBroadcast}
			if len(venue.broadcastQueue) > 0 {
				script, venue.broadcastQueue = venue.broadcastQueue[0], venue.broadcastQueue[1:]
			}
			venue.mu.Unlock()
			if script.delay > 0 {
				time.Sleep(script.delay)
			}
			_, _ = io.WriteString(w, script.body)
		default:
			t.Errorf("unexpected rpc method %q", request.Method)
		}
	})
	mux.HandleFunc("/v4/ws", func(w http.ResponseWriter, r *http.Request) {
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
			frame := string(data)
			venue.mu.Lock()
			venue.subscribeFrames = append(venue.subscribeFrames, frame)
			venue.mu.Unlock()
			if strings.Contains(frame, `"subscribe"`) {
				venue.pushSubscribed()
			}
		}
	})

	venue.server = httptest.NewServer(mux)
	venue.wsURL = "ws" + strings.TrimPrefix(venue.server.URL, "http") + "/v4/ws"
	t.Cleanup(venue.server.Close)
	return venue
}

// accountQueryResponse encodes the abci_query answer for an account lookup.
func (v *fakeVenue) accountQueryResponse(t *testing.T) string {
	t.Helper()
	baseAccount, err := marshal(&authpb.BaseAccount{
		Address:       testAddress,
		AccountNumber: testAccountNum,
		Sequence:      testSequence,
	})
	if err != nil {
		t.Fatalf("marshal BaseAccount: %v", err)
	}
	response, err := marshal(&authpb.QueryAccountResponse{
		Account: &anypb.Any{TypeUrl: baseAccountTypeURL, Value: baseAccount},
	})
	if err != nil {
		t.Fatalf("marshal QueryAccountResponse: %v", err)
	}
	return fmt.Sprintf(`{"result":{"response":{"code":0,"value":"%s"}}}`,
		base64.StdEncoding.EncodeToString(response))
}

// pushSubscribed answers a subscription with the snapshot the venue sends.
func (v *fakeVenue) pushSubscribed() {
	v.mu.Lock()
	subaccount := v.subaccountJSON
	v.mu.Unlock()

	var snapshot map[string]any
	if err := json.Unmarshal(subaccount, &snapshot); err != nil {
		v.t.Errorf("subaccount fixture decode: %v", err)
		return
	}
	frame, err := json.Marshal(map[string]any{
		"type":    wsTypeSubscribed,
		"channel": subaccountsChannel,
		"id":      testAddress + "/0",
		"contents": map[string]any{
			"subaccount":  snapshot["subaccount"],
			"orders":      []any{},
			"blockHeight": strconv.FormatUint(uint64(testHeight), 10),
		},
	})
	if err != nil {
		v.t.Errorf("subscribed frame encode: %v", err)
		return
	}
	v.push(frame)
}

// push writes a frame to the newest connection. Writes are serialized under
// writeMu because both the test goroutine and the server handler push.
func (v *fakeVenue) push(frame []byte) {
	v.mu.Lock()
	if len(v.conns) == 0 {
		v.mu.Unlock()
		v.t.Error("no ws connection to push into")
		return
	}
	conn := v.conns[len(v.conns)-1]
	v.mu.Unlock()

	v.writeMu.Lock()
	defer v.writeMu.Unlock()
	if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		v.t.Errorf("push: %v", err)
	}
}

func (v *fakeVenue) queueBroadcast(scripts ...scriptedBroadcast) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.broadcastQueue = append(v.broadcastQueue, scripts...)
}

func (v *fakeVenue) broadcastCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.broadcastTxs)
}

func (v *fakeVenue) setHeight(height uint32) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.height = height
}

func (v *fakeVenue) setHeightFailing(failing bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.heightErr = failing
}

// blockMarkets holds the market-metadata response until the returned function
// is called, so a test can run something else while Connect is mid-flight.
func (v *fakeVenue) blockMarkets() func() {
	gate := make(chan struct{})
	v.mu.Lock()
	v.marketsGate = gate
	v.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

func (v *fakeVenue) setMarkets(body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.marketsJSON = []byte(body)
}

// setFillHistory switches the fills endpoint to a paginating one, so a backfill
// that must walk back through more than one page is actually exercised.
func (v *fakeVenue) setFillHistory(fills []map[string]any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fillHistory = fills
}

// fillPage serves one page of history, newest first, honoring the
// createdBeforeOrAt cursor and limit the adapter sends.
func (v *fakeVenue) fillPage(t *testing.T, history []map[string]any, query url.Values) []byte {
	t.Helper()
	v.mu.Lock()
	v.fillPageQueries = append(v.fillPageQueries, query.Encode())
	v.mu.Unlock()

	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil {
		t.Errorf("fills limit %q: %v", query.Get("limit"), err)
		limit = fillBackfillLimit
	}
	cursor := query.Get("createdBeforeOrAt")

	page := make([]map[string]any, 0, limit)
	for _, entry := range history {
		if cursor != "" && entry["createdAt"].(string) > cursor {
			continue
		}
		page = append(page, entry)
		if len(page) == limit {
			break
		}
	}
	body, err := json.Marshal(map[string]any{"fills": page})
	if err != nil {
		t.Errorf("encode fills page: %v", err)
	}
	return body
}

func (v *fakeVenue) fillPageCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.fillPageQueries)
}

// setSubaccount replaces the account snapshot the REST endpoint serves, so a
// test can control what an out-of-band re-read finds.
func (v *fakeVenue) setSubaccount(body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.subaccountJSON = []byte(body)
}

// queueSubaccount serves these snapshots to the next reads, one each, before
// falling back to the standing one — so a test can hand back a state that
// settles partway through a sequence of reads.
func (v *fakeVenue) queueSubaccount(bodies ...string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.subaccountQueue = append(v.subaccountQueue, bodies...)
}

func (v *fakeVenue) subaccountReadCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.subaccountReads
}

// blockSubaccount holds the account snapshot response until the returned
// function is called, so a test can run the stream while a re-read is in
// flight.
func (v *fakeVenue) blockSubaccount() func() {
	gate := make(chan struct{})
	v.mu.Lock()
	v.subaccountGate = gate
	v.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

func (v *fakeVenue) setFills(body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fillsJSON = []byte(body)
}

// recordingSigner satisfies the signer seam without touching real crypto.
type recordingSigner struct {
	addr string

	mu          sync.Mutex
	placeCalls  []placeOrderParams
	cancelCalls []cancelOrderParams
	envelopes   []txParams
	signErr     error
}

func (s *recordingSigner) signPlaceOrder(params placeOrderParams, envelope txParams) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.placeCalls = append(s.placeCalls, params)
	s.envelopes = append(s.envelopes, envelope)
	if s.signErr != nil {
		return nil, s.signErr
	}
	return []byte("fake-place-tx"), nil
}

func (s *recordingSigner) signCancelOrder(params cancelOrderParams, envelope txParams) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelCalls = append(s.cancelCalls, params)
	s.envelopes = append(s.envelopes, envelope)
	if s.signErr != nil {
		return nil, s.signErr
	}
	return []byte("fake-cancel-tx"), nil
}

func (s *recordingSigner) address() string { return s.addr }
func (s *recordingSigner) pubKey() []byte  { return make([]byte, compressedPubKeyLen) }

func (s *recordingSigner) lastPlace(t *testing.T) placeOrderParams {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.placeCalls) == 0 {
		t.Fatal("no place-order call was signed")
	}
	return s.placeCalls[len(s.placeCalls)-1]
}

func newTestExecutor(t *testing.T, venue *fakeVenue) (*Executor, *recordingSigner, *smoketest.Collector) {
	t.Helper()
	fake := &recordingSigner{addr: testAddress}
	executor, err := New(Config{
		Credentials: Credentials{
			PrivateKeyHex:    testPrivateKeyHex,
			Address:          testAddress,
			SubaccountNumber: 0,
		},
		Symbol:  testSymbol,
		Ticker:  testTicker,
		Network: Testnet,
		Reconnect: godex.ReconnectConfig{
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     100 * time.Millisecond,
			Multiplier:   2,
			IdleTimeout:  time.Second,
		},
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		IndexerRESTBaseURL:   venue.server.URL + "/v4",
		IndexerWSURL:         venue.wsURL,
		RPCBaseURL:           venue.server.URL,
		TxRequestTimeout:     500 * time.Millisecond,
		TxFaultRecoveryDelay: 100 * time.Millisecond,
		HeightPollInterval:   50 * time.Millisecond,
		HeightStaleAfter:     2 * time.Second,
		newSigner:            func(*resolvedConfig) (signer, error) { return fake, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	collector := smoketest.NewCollector(t.Logf)
	consumed := make(chan struct{})
	go func() {
		collector.Consume(executor.AccountEvents())
		close(consumed)
	}()
	t.Cleanup(func() {
		_ = executor.Close()
		<-consumed
	})
	return executor, fake, collector
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
		Symbol: testSymbol,
		Side:   godex.SideBuy,
		Price:  decimal.MustFromString("3000.17", 2),
		Size:   decimal.MustFromString("0.5009", 4),
		Intent: intent,
	}
}

func isConnectedEvent(e godex.AccountEvent) bool { _, ok := e.(godex.ConnectedEvent); return ok }
func isDisconnectedEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.DisconnectedEvent)
	return ok
}
func isPositionEvent(e godex.AccountEvent) bool { _, ok := e.(godex.PositionEvent); return ok }
func isMarginEvent(e godex.AccountEvent) bool   { _, ok := e.(godex.MarginEvent); return ok }

func isFillEvent(e godex.AccountEvent) bool { _, ok := e.(godex.FillEvent); return ok }

func isRejectionFor(id godex.OrderID) func(godex.AccountEvent) bool {
	return func(e godex.AccountEvent) bool {
		rejected, ok := e.(godex.OrderRejectedEvent)
		return ok && rejected.OrderID == id
	}
}

func TestConnectEmitsVerifiedSnapshot(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	metadata := mustConnect(t, executor)

	if got := metadata.SizeStep.String(); got != "0.001" {
		t.Fatalf("sizeStep = %s, want 0.001", got)
	}
	// dYdX publishes the maintenance margin fraction as a plain ratio.
	if got := metadata.MaintenanceMarginFraction.String(); got != "0.03" {
		t.Fatalf("mmf = %s, want 0.03", got)
	}

	ctx := context.Background()
	if _, err := collector.WaitFor(ctx, 0, testEventTimeout, "connected", isConnectedEvent); err != nil {
		t.Fatal(err)
	}
	position, err := collector.WaitFor(ctx, 0, testEventTimeout, "position snapshot", isPositionEvent)
	if err != nil {
		t.Fatal(err)
	}
	if got := position.(godex.PositionEvent).Position.Size.String(); got != "0.500" {
		t.Fatalf("position size = %s", got)
	}
	margin, err := collector.WaitFor(ctx, 0, testEventTimeout, "margin snapshot", isMarginEvent)
	if err != nil {
		t.Fatal(err)
	}
	if got := margin.(godex.MarginEvent).UsageRatio.String(); got != "0.0800" {
		t.Fatalf("usage ratio = %s", got)
	}

	venue.mu.Lock()
	frames := append([]string(nil), venue.subscribeFrames...)
	venue.mu.Unlock()
	if len(frames) == 0 || !strings.Contains(frames[0], subaccountsChannel) {
		t.Fatalf("expected a v4_subaccounts subscription, got %v", frames)
	}
	if !strings.Contains(frames[0], testAddress+"/0") {
		t.Fatalf("subscription id should name the subaccount: %s", frames[0])
	}
}

// TestConnectFailsOnAddressMismatch guards against signing orders for an
// account the configured key does not control.
func TestConnectFailsOnAddressMismatch(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	fake.addr = "dydx1tw5zd8wefzwd28pnja2n2mn0yalf68jjttrkdu"

	_, err := executor.Connect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "signing key controls") {
		t.Fatalf("expected an address mismatch failure, got %v", err)
	}
}

// TestConnectAcceptsScopedAuthenticatorKey is the whole point of the
// authenticator support: the in-process key is deliberately *not* the account
// owner's, so requiring the addresses to match would reject the one
// configuration that keeps a withdrawal-capable key out of this process.
func TestConnectAcceptsScopedAuthenticatorKey(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	authenticatorID := uint64(11)
	executor.cfg.credentials.AuthenticatorID = &authenticatorID
	fake.addr = "dydx1tw5zd8wefzwd28pnja2n2mn0yalf68jjttrkdu"

	if _, err := executor.Connect(context.Background()); err != nil {
		t.Fatalf("Connect with a scoped authenticator key: %v", err)
	}
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	params := fake.lastPlace(t)
	// The order still belongs to the owner account, not the signing key.
	if params.address != testAddress {
		t.Fatalf("order owner = %s, want the configured account %s", params.address, testAddress)
	}
	fake.mu.Lock()
	envelope := fake.envelopes[0]
	fake.mu.Unlock()
	if envelope.authenticatorID == nil || *envelope.authenticatorID != authenticatorID {
		t.Fatalf("authenticator id = %v, want %d", envelope.authenticatorID, authenticatorID)
	}
}

func TestConnectFailsOnUnusableMarket(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		ticker string
	}{
		{name: "not listed", ticker: "DOGE-USD"},
		{name: "not active", ticker: "PAUSED-USD"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			venue := newFakeVenue(t)
			executor, _, _ := newTestExecutor(t, venue)
			executor.cfg.ticker = testCase.ticker

			if _, err := executor.Connect(context.Background()); err == nil {
				t.Fatal("expected Connect to fail")
			}
		})
	}
}

// TestConnectFailsBeforeStartingAnythingOnUnusableMetadata: a market payload
// that decodes but carries an unusable decimal must fail Connect cleanly. If
// the check happened after the stream and pollers were up, a failed Connect
// would leave goroutines running and transactions being accepted.
func TestConnectFailsBeforeStartingAnythingOnUnusableMetadata(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setMarkets(`{"markets":{"ETH-USD":{
		"ticker":"ETH-USD","clobPairId":"1","status":"ACTIVE",
		"tickSize":"0.1","stepSize":"0.001",
		"atomicResolution":-9,"quantumConversionExponent":-9,
		"stepBaseQuantums":1000000,"subticksPerTick":100000,
		"maintenanceMarginFraction":"three percent"}}}`)
	executor, _, collector := newTestExecutor(t, venue)

	_, err := executor.Connect(context.Background())
	if err == nil || !strings.Contains(err.Error(), "maintenanceMarginFraction") {
		t.Fatalf("expected a metadata parse failure, got %v", err)
	}
	// Nothing may have been started: no stream, so no connection event.
	for _, event := range collector.Events() {
		if _, ok := event.(godex.ConnectedEvent); ok {
			t.Fatal("a failed Connect must not have opened the account stream")
		}
	}
	// And the executor must not accept orders.
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); err == nil {
		t.Fatal("a failed Connect must leave the executor unable to trade")
	}
}

// TestConnectFailsWhenHeightUnavailable: without a height there is no valid
// good_til_block, so connecting would produce an executor that cannot trade.
func TestConnectFailsWhenHeightUnavailable(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setHeightFailing(true)
	executor, _, _ := newTestExecutor(t, venue)

	if _, err := executor.Connect(context.Background()); err == nil {
		t.Fatal("expected Connect to fail without a block height")
	}
}

func TestPlaceOrderPostOnly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != godex.AckSubmitted || ack.VenueID != godex.VenueDydx {
		t.Fatalf("unexpected ack: %+v", ack)
	}

	params := fake.lastPlace(t)
	// Buy price 3000.17 floors to the 0.1 tick -> 3000.1 -> 3000100000 subticks;
	// size 0.5009 floors to the 0.001 step -> 0.500 -> 500000000 quantums.
	if params.subticks != 3_000_100_000 {
		t.Fatalf("subticks = %d, want 3000100000", params.subticks)
	}
	if params.quantums != 500_000_000 {
		t.Fatalf("quantums = %d, want 500000000", params.quantums)
	}
	if params.side != sideBuy || params.timeInForce != timeInForcePostOnly || params.reduceOnly {
		t.Fatalf("unexpected order params: %+v", params)
	}
	if params.clobPairID != 1 || params.address != testAddress {
		t.Fatalf("unexpected order identity: %+v", params.orderIdentity)
	}
	if params.goodTilBlock != testHeight+shortBlockForward {
		t.Fatalf("good_til_block = %d, want %d", params.goodTilBlock, testHeight+shortBlockForward)
	}
	if ack.OrderID != godex.OrderID(strconv.FormatUint(uint64(params.clientID), 10)) {
		t.Fatalf("ack id %s does not match client id %d", ack.OrderID, params.clientID)
	}

	fake.mu.Lock()
	envelope := fake.envelopes[0]
	fake.mu.Unlock()
	if envelope.accountNumber != testAccountNum || envelope.sequence != testSequence {
		t.Fatalf("envelope = %+v, want the account read at Connect", envelope)
	}
	if envelope.chainID != testnetChainID {
		t.Fatalf("chain id = %q", envelope.chainID)
	}
}

func TestPlaceOrderIOCReduceOnly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	order := testOrder(godex.IntentIOC)
	order.Side = godex.SideSell
	order.ReduceOnly = true
	if _, err := executor.PlaceOrder(context.Background(), order); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	params := fake.lastPlace(t)
	if params.side != sideSell || params.timeInForce != timeInForceIOC || !params.reduceOnly {
		t.Fatalf("unexpected order params: %+v", params)
	}
	// A sell price ceils to the tick: 3000.17 -> 3000.2.
	if params.subticks != 3_000_200_000 {
		t.Fatalf("subticks = %d, want 3000200000", params.subticks)
	}
}

// TestPlaceOrderRejectsReduceOnlyPostOnly: the chain only honors reduce-only on
// orders that cannot rest, so submitting one would silently drop the flag.
func TestPlaceOrderRejectsReduceOnlyPostOnly(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	order := testOrder(godex.IntentPostOnly)
	order.ReduceOnly = true
	_, err := executor.PlaceOrder(context.Background(), order)
	if err == nil || !strings.Contains(err.Error(), "reduce-only") {
		t.Fatalf("expected a reduce-only rejection, got %v", err)
	}
	if venue.broadcastCount() != 0 {
		t.Fatal("nothing should have been broadcast")
	}
}

func TestPlaceOrderRejectsWrongSymbol(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	order := testOrder(godex.IntentPostOnly)
	order.Symbol = "BTC-PERP"
	if _, err := executor.PlaceOrder(context.Background(), order); err == nil {
		t.Fatal("expected an error for a symbol this executor is not configured for")
	}
}

// TestPlaceOrderPostOnlyCrossingIsRejectedNotAnError is the contract's central
// normal-path case: a post-only order that would take liquidity comes back as a
// rejected ack plus an event, never as a Go error.
func TestPlaceOrderPostOnlyCrossingIsRejectedNotAnError(t *testing.T) {
	venue := newFakeVenue(t)
	venue.queueBroadcast(scriptedBroadcast{body: `{"jsonrpc":"2.0","id":-1,"result":{"code":2003,` +
		`"codespace":"clob","log":"Post-only order would cross one or more maker orders","hash":"DEF"}}`})
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("a crossing post-only must not surface as an error: %v", err)
	}
	if ack.Status != godex.AckRejected {
		t.Fatalf("status = %s, want rejected", ack.Status)
	}
	rejection, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"order rejected", isRejectionFor(ack.OrderID))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rejection.(godex.OrderRejectedEvent).Reason, "Post-only") {
		t.Fatalf("reason = %q", rejection.(godex.OrderRejectedEvent).Reason)
	}
	// The order never rested, so it must not remain cancellable.
	if err := executor.CancelOrder(context.Background(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("cancel after rejection = %v, want ErrUnknownOrder", err)
	}
}

func TestPlaceOrderOtherRejectionIsAnError(t *testing.T) {
	venue := newFakeVenue(t)
	venue.queueBroadcast(scriptedBroadcast{body: `{"jsonrpc":"2.0","id":-1,"result":{"code":11,` +
		`"codespace":"sdk","log":"out of gas","hash":"DEF"}}`})
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); err == nil {
		t.Fatal("expected an error for a non-post-only rejection")
	}
}

// TestPlaceOrderUnknownOutcomeLatchesFaultAndRecovers covers the never-retry
// rule: a broadcast whose answer never arrives halts trading, and the adapter
// waits until the ambiguous order can no longer be live before concluding
// anything about it.
func TestPlaceOrderUnknownOutcomeLatchesFaultAndRecovers(t *testing.T) {
	venue := newFakeVenue(t)
	venue.queueBroadcast(scriptedBroadcast{body: acceptedBroadcast, delay: 750 * time.Millisecond})
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	_, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrTxOutcomeUnknown", err)
	}
	expiryBlock := fake.lastPlace(t).goodTilBlock
	broadcasts := venue.broadcastCount()

	// While the fault is latched no further transaction may be sent.
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("second PlaceOrder = %v, want ErrTxOutcomeUnknown", err)
	}
	if venue.broadcastCount() != broadcasts {
		t.Fatal("a latched fault must not resend anything")
	}

	// The Indexer showing no open order proves nothing yet: the chain may have
	// accepted the order moments ago and not indexed it. Until its expiry block
	// passes, the adapter must neither write the order off nor resume trading.
	time.Sleep(4 * executor.cfg.txFaultRecoveryDelay)
	for _, event := range collector.Events()[mark:] {
		if _, ok := event.(godex.OrderRejectedEvent); ok {
			t.Fatal("the order was written off while it could still be live")
		}
	}
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("PlaceOrder before the expiry block = %v, want ErrTxOutcomeUnknown", err)
	}

	// Past the expiry block the order cannot be live under any interpretation,
	// so the venue's silence about it is now conclusive.
	venue.setHeight(expiryBlock + 1)
	waitForHeight(t, executor, expiryBlock+1)

	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"reconciliation rejection", func(e godex.AccountEvent) bool {
			rejected, ok := e.(godex.OrderRejectedEvent)
			return ok && strings.Contains(rejected.Reason, "unknown submission outcome")
		}); err != nil {
		t.Fatal(err)
	}

	// Recovery clears the latch and trading resumes.
	deadline := time.Now().Add(testEventTimeout)
	for {
		_, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
		if err == nil {
			break
		}
		if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
			t.Fatalf("PlaceOrder after recovery: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("fault never cleared")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestUnknownOutcomeReportsAFillBeforeWritingTheOrderOff: if the ambiguous
// order did land and fill, that execution must reach the consumer — writing the
// order off without it would leave the strategy with a position it never saw
// open.
func TestUnknownOutcomeReportsAFillBeforeWritingTheOrderOff(t *testing.T) {
	venue := newFakeVenue(t)
	venue.queueBroadcast(scriptedBroadcast{body: acceptedBroadcast, delay: 750 * time.Millisecond})
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentIOC)); !errors.Is(err, godex.ErrTxOutcomeUnknown) {
		t.Fatalf("expected ErrTxOutcomeUnknown, got %v", err)
	}
	expiryBlock := fake.lastPlace(t).goodTilBlock

	// The order did reach the chain and filled; the venue records it while the
	// adapter is still unsure.
	fills, err := json.Marshal(map[string]any{"fills": []any{
		fillObject("fill-from-ambiguous-order", "venue-order-ambiguous", "3010.2", "0.100"),
	}})
	if err != nil {
		t.Fatalf("encode fills: %v", err)
	}
	venue.setFills(string(fills))
	venue.setHeight(expiryBlock + 1)
	waitForHeight(t, executor, expiryBlock+1)

	fill, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatal(err)
	}
	if fill.(godex.FillEvent).Size.String() != "0.100" {
		t.Fatalf("fill size = %s", fill.(godex.FillEvent).Size)
	}
}

// TestPlaceOrderFailsOnStaleHeight: rather than extrapolate, an unknown height
// stops submissions, because a wrong good_til_block can produce an order that
// looks accepted but is already expired.
func TestPlaceOrderFailsOnStaleHeight(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// Freeze the tracker in the past and stop the poller from healing it.
	executor.height.mu.Lock()
	executor.height.fetchedAt = time.Now().Add(-time.Hour)
	executor.height.mu.Unlock()
	venue.setHeightFailing(true)

	_, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected a stale-height failure, got %v", err)
	}
}

func TestCancelOrderUnknownID(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	if err := executor.CancelOrder(context.Background(), "nope"); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("err = %v, want ErrUnknownOrder", err)
	}
}

func TestCancelOrderSignsTrackedOrder(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if err := executor.CancelOrder(context.Background(), ack.OrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}

	fake.mu.Lock()
	cancels := append([]cancelOrderParams(nil), fake.cancelCalls...)
	fake.mu.Unlock()
	if len(cancels) != 1 {
		t.Fatalf("got %d cancel signatures, want 1", len(cancels))
	}
	if strconv.FormatUint(uint64(cancels[0].clientID), 10) != string(ack.OrderID) {
		t.Fatalf("cancel client id %d does not match order %s", cancels[0].clientID, ack.OrderID)
	}
	// A second cancel has nothing to act on.
	if err := executor.CancelOrder(context.Background(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("second cancel = %v, want ErrUnknownOrder", err)
	}
}

// TestCancelOrderAfterExpiryIsIdempotent: a short-term order past its expiry
// block is already gone, so canceling it must succeed quietly instead of
// failing against a venue that has forgotten it.
func TestCancelOrderAfterExpiryIsIdempotent(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	broadcasts := venue.broadcastCount()

	// Advance the chain past the order's good_til_block.
	venue.setHeight(testHeight + shortBlockForward + 5)
	waitForHeight(t, executor, testHeight+shortBlockForward+5)

	if err := executor.CancelOrder(context.Background(), ack.OrderID); err != nil {
		t.Fatalf("cancel of an expired order should be a no-op, got %v", err)
	}
	if venue.broadcastCount() != broadcasts {
		t.Fatal("an expired order must not be cancelled on chain")
	}
	fake.mu.Lock()
	cancelCount := len(fake.cancelCalls)
	fake.mu.Unlock()
	if cancelCount != 0 {
		t.Fatal("no cancel transaction should have been signed")
	}
}

func waitForHeight(t *testing.T, executor *Executor, want uint32) {
	t.Helper()
	deadline := time.Now().Add(testEventTimeout)
	for {
		if height, err := executor.height.current(); err == nil && height >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("block height never reached %d", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestOrderExpirySurfacesAsRejection is how a strategy learns a short-term
// quote is gone: the contract has no expiry event, so expiry is a rejection.
func TestOrderExpirySurfacesAsRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	params := fake.lastPlace(t)

	mark := collector.Mark()
	venue.push(orderRemovalFrame(t, params.clientID, params.goodTilBlock, removalReasonExpired))

	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"expiry rejection", isRejectionFor(ack.OrderID))
	if err != nil {
		t.Fatal(err)
	}
	reason := event.(godex.OrderRejectedEvent).Reason
	want := fmt.Sprintf("expired: good_til_block %d reached", params.goodTilBlock)
	if reason != want {
		t.Fatalf("reason = %q, want %q", reason, want)
	}
	// The order is gone; cancelling it is no longer meaningful.
	if err := executor.CancelOrder(context.Background(), ack.OrderID); !errors.Is(err, godex.ErrUnknownOrder) {
		t.Fatalf("cancel after expiry = %v, want ErrUnknownOrder", err)
	}
}

func orderRemovalFrame(t *testing.T, clientID, goodTilBlock uint32, reason string) []byte {
	t.Helper()
	return orderFrame(t, clientID, "venue-order-"+strconv.FormatUint(uint64(clientID), 10),
		orderStatusCanceled, map[string]any{
			"goodTilBlock":  strconv.FormatUint(uint64(goodTilBlock), 10),
			"removalReason": reason,
		})
}

// orderFrame builds one channel_data order update, with any extra fields the
// case needs merged in.
func orderFrame(t *testing.T, clientID uint32, venueOrderID, status string, extra map[string]any) []byte {
	t.Helper()
	order := map[string]any{
		"id":         venueOrderID,
		"clientId":   strconv.FormatUint(uint64(clientID), 10),
		"clobPairId": "1",
		"side":       "BUY",
		"status":     status,
	}
	for key, value := range extra {
		order[key] = value
	}
	frame, err := json.Marshal(map[string]any{
		"type":     wsTypeChannelData,
		"channel":  subaccountsChannel,
		"contents": map[string]any{"orders": []any{order}},
	})
	if err != nil {
		t.Fatalf("encode order frame: %v", err)
	}
	return frame
}

// TestFillFromStreamIsAttributedToItsOrder relies on the venue order id learned
// from the order update that precedes the fill.
func TestFillFromStreamIsAttributedToItsOrder(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentIOC))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	params := fake.lastPlace(t)
	venueOrderID := "venue-order-" + strconv.FormatUint(uint64(params.clientID), 10)

	mark := collector.Mark()
	// The order update teaches the executor the venue's id for this order...
	venue.push(orderOpenFrame(t, params.clientID, venueOrderID))
	// ...so the fill that follows is attributed to the caller's order id.
	venue.push(fillFrame(t, "fill-1", venueOrderID, "3010.2", "0.100"))

	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatal(err)
	}
	fillEvent := event.(godex.FillEvent)
	if fillEvent.OrderID != ack.OrderID {
		t.Fatalf("fill order id = %s, want %s", fillEvent.OrderID, ack.OrderID)
	}
	if fillEvent.Price.String() != "3010.2" || fillEvent.Size.String() != "0.100" {
		t.Fatalf("fill price/size = %s/%s", fillEvent.Price, fillEvent.Size)
	}
}

func orderOpenFrame(t *testing.T, clientID uint32, venueOrderID string) []byte {
	t.Helper()
	return orderFrame(t, clientID, venueOrderID, orderStatusOpen, nil)
}

func fillFrame(t *testing.T, fillID, venueOrderID, price, size string) []byte {
	t.Helper()
	frame, err := json.Marshal(map[string]any{
		"type":    wsTypeChannelData,
		"channel": subaccountsChannel,
		"contents": map[string]any{
			"fills": []any{streamFillObject(fillID, venueOrderID, price, size)},
		},
	})
	if err != nil {
		t.Fatalf("encode fill frame: %v", err)
	}
	return frame
}

// fillObject builds a fill as the REST history reports one, which names the
// market "market".
func fillObject(fillID, venueOrderID, price, size string) map[string]any {
	return map[string]any{
		"id":        fillID,
		"side":      "BUY",
		"market":    testTicker,
		"price":     price,
		"size":      size,
		"createdAt": "2026-07-25T11:05:03.421Z",
		"orderId":   venueOrderID,
	}
}

// streamFillObject builds a fill as the account stream reports one. The venue
// names the market "ticker" there, not "market" (testnet capture, 2026-07-27).
func streamFillObject(fillID, venueOrderID, price, size string) map[string]any {
	entry := fillObject(fillID, venueOrderID, price, size)
	delete(entry, "market")
	entry["ticker"] = testTicker
	entry["clobPairId"] = "1"
	return entry
}

// TestConnectDoesNotReplayHistoricalFills: the account's existing fills predate
// this executor, so emitting them would have a consumer book old executions as
// new ones. The first subscription absorbs them as a watermark instead.
func TestConnectDoesNotReplayHistoricalFills(t *testing.T) {
	venue := newFakeVenue(t)
	venue.setFills(string(loadFixture(t, "fills.json")))
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// The margin event closes the connect snapshot, so anything the connect
	// path would emit has been emitted by now.
	if _, err := collector.WaitFor(context.Background(), 0, testEventTimeout,
		"margin snapshot", isMarginEvent); err != nil {
		t.Fatal(err)
	}
	for _, event := range collector.Events() {
		if fill, ok := event.(godex.FillEvent); ok {
			t.Fatalf("historical fill replayed on connect: %+v", fill)
		}
	}

	// A fill that arrives after connect is still reported.
	mark := collector.Mark()
	venue.push(fillFrame(t, "fill-new", "venue-order-z", "3010.2", "0.100"))
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"live fill", isFillEvent); err != nil {
		t.Fatal(err)
	}
}

// TestReconnectBackfillsFillsWithoutDuplication is the reconnect safety
// property: nothing that already reached the consumer is replayed, and anything
// that happened while the stream was down still arrives.
func TestReconnectBackfillsFillsWithoutDuplication(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push(fillFrame(t, "fill-live", "venue-order-x", "3010.2", "0.100"))
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "live fill", isFillEvent); err != nil {
		t.Fatal(err)
	}

	// The backfill re-reports the live fill and adds one that landed while the
	// stream was down.
	backfill, err := json.Marshal(map[string]any{"fills": []any{
		fillObject("fill-missed", "venue-order-y", "3011.5", "0.200"),
		fillObject("fill-live", "venue-order-x", "3010.2", "0.100"),
	}})
	if err != nil {
		t.Fatalf("encode backfill: %v", err)
	}
	venue.setFills(string(backfill))

	reconnectMark := collector.Mark()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	if _, err := collector.WaitFor(context.Background(), reconnectMark, testEventTimeout,
		"missed fill backfilled", func(e godex.AccountEvent) bool {
			fill, ok := e.(godex.FillEvent)
			return ok && fill.Size.String() == "0.200"
		}); err != nil {
		t.Fatal(err)
	}
	// Let the reconnect settle so any duplicate would have been delivered.
	if _, err := collector.WaitFor(context.Background(), reconnectMark, testEventTimeout,
		"position after reconnect", isPositionEvent); err != nil {
		t.Fatal(err)
	}

	liveFills := 0
	for _, event := range collector.Events() {
		if fill, ok := event.(godex.FillEvent); ok && fill.Size.String() == "0.100" {
			liveFills++
		}
	}
	if liveFills != 1 {
		t.Fatalf("the already-delivered fill was emitted %d times, want 1", liveFills)
	}
}

// TestReconnectPagesBackfillPastOnePage: an outage longer than one page of
// fills must still be reconciled in full. Keeping only the newest page would
// drop executions while reporting the account as converged.
func TestReconnectPagesBackfillPastOnePage(t *testing.T) {
	venue := newFakeVenue(t)
	// One pre-existing fill so connect establishes a floor to page back to.
	base := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	history := []map[string]any{fillHistoryEntry("fill-old", base)}
	venue.setFillHistory(history)

	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)
	if _, err := collector.WaitFor(context.Background(), 0, testEventTimeout,
		"margin snapshot", isMarginEvent); err != nil {
		t.Fatal(err)
	}

	// The stream goes down and the venue records more fills than one page holds.
	const missed = fillBackfillLimit + 37
	fresh := make([]map[string]any, 0, missed+1)
	for i := missed; i >= 1; i-- {
		fresh = append(fresh, fillHistoryEntry(
			fmt.Sprintf("fill-missed-%d", i), base.Add(time.Duration(i)*time.Second)))
	}
	venue.setFillHistory(append(fresh, history...))

	mark := collector.Mark()
	pagesBefore := venue.fillPageCount()
	if err := executor.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	// Fills are emitted oldest first, so the newest one arriving means the whole
	// backfill has been delivered.
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"newest missed fill", func(e godex.AccountEvent) bool {
			fill, ok := e.(godex.FillEvent)
			return ok && fill.Time.Equal(base.Add(missed*time.Second))
		}); err != nil {
		t.Fatal(err)
	}
	var fills []godex.FillEvent
	for _, event := range collector.Events()[mark:] {
		if fill, ok := event.(godex.FillEvent); ok {
			fills = append(fills, fill)
		}
	}
	// Chronological order: the oldest missed fill leads.
	if len(fills) > 0 && !fills[0].Time.Equal(base.Add(time.Second)) {
		t.Fatalf("first backfilled fill is at %s, want the oldest at %s",
			fills[0].Time, base.Add(time.Second))
	}
	if len(fills) != missed {
		t.Fatalf("backfilled %d fills, want %d", len(fills), missed)
	}
	if pages := venue.fillPageCount() - pagesBefore; pages < 2 {
		t.Fatalf("backfill used %d page requests; it cannot have covered %d fills", pages, missed)
	}
}

func fillHistoryEntry(id string, createdAt time.Time) map[string]any {
	entry := fillObject(id, "venue-order-"+id, "3010.2", "0.100")
	entry["createdAt"] = createdAt.UTC().Format(time.RFC3339)
	return entry
}

// TestReconnectFailsRatherThanTruncateAnUnreconcilableBackfill: when the gap is
// larger than the adapter can walk, it must say so instead of silently keeping
// the newest fills and calling the account converged.
func TestReconnectFailsRatherThanTruncateAnUnreconcilableBackfill(t *testing.T) {
	venue := newFakeVenue(t)
	base := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	venue.setFillHistory([]map[string]any{fillHistoryEntry("fill-old", base)})

	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// More fills than maxFillBackfillPages pages can cover, none of them known.
	total := maxFillBackfillPages*fillBackfillLimit + 1
	history := make([]map[string]any, 0, total)
	for i := total; i >= 1; i-- {
		history = append(history, fillHistoryEntry(
			fmt.Sprintf("fill-%d", i), base.Add(time.Duration(i)*time.Second)))
	}
	venue.setFillHistory(history)

	err := executor.backfillFills(context.Background())
	if err == nil || !strings.Contains(err.Error(), "partial convergence") {
		t.Fatalf("expected a refusal to report partial convergence, got %v", err)
	}
}

// TestUnknownStreamMessageAbortsIntoReconnect: an unrecognized payload means
// the adapter no longer understands the venue, so it drops the connection
// rather than guessing.
func TestUnknownStreamMessageAbortsIntoReconnect(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	venue.push([]byte(`{"type":"something_new","channel":"v4_subaccounts"}`))

	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"disconnected", isDisconnectedEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"reconnected", isConnectedEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"snapshot after reconnect", isPositionEvent); err != nil {
		t.Fatal(err)
	}
}

// TestPartiallyFilledIOCIsStillAttributedAfterItsRejection pins what a
// rejection does and does not mean.
//
// An IOC that fills part of its size has the remainder cancelled, and the venue
// reports that cancellation in an earlier message than the execution — the
// order observed on testnet. So the rejection closes the order before its own
// fill arrives, and the fill must still reach the caller under the order id
// they were given. That works because the venue-id mapping deliberately
// outlives the order; removing it with the order would silently reattribute
// this fill to a venue id the caller has never seen.
func TestPartiallyFilledIOCIsStillAttributedAfterItsRejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentIOC))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	params := fake.lastPlace(t)
	venueOrderID := "venue-order-" + strconv.FormatUint(uint64(params.clientID), 10)

	mark := collector.Mark()
	venue.push(orderFrame(t, params.clientID, venueOrderID, orderStatusCanceled, map[string]any{
		"removalReason": "ORDER_REMOVAL_REASON_IMMEDIATE_OR_CANCEL_WOULD_REST_ON_BOOK",
	}))
	venue.push(fillFrame(t, "fill-ioc-remainder", venueOrderID, "3010.2", "0.002"))

	rejection, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"remainder cancelled", isRejectionFor(ack.OrderID))
	if err != nil {
		t.Fatal(err)
	}
	if reason := rejection.(godex.OrderRejectedEvent).Reason; !strings.Contains(reason, "IMMEDIATE_OR_CANCEL") {
		t.Fatalf("reason = %q, want the venue's own removal reason", reason)
	}

	fill, err := collector.WaitFor(context.Background(), mark, testEventTimeout, "fill", isFillEvent)
	if err != nil {
		t.Fatal(err)
	}
	filled := fill.(godex.FillEvent)
	if filled.OrderID != ack.OrderID {
		t.Fatalf("fill attributed to %q, want the caller's order id %q — a rejection must not "+
			"orphan a fill that follows it", filled.OrderID, ack.OrderID)
	}
	if filled.Size.String() != "0.002" {
		t.Fatalf("fill size = %s, want the partially filled 0.002", filled.Size)
	}
}

// TestFilledOrdersAreRetiredWithoutARejection: a fully filled order is terminal
// too. It must stop being tracked — otherwise every IOC leaks an entry for the
// life of the process — but reporting it as rejected would contradict the fills
// already delivered.
func TestFilledOrdersAreRetiredWithoutARejection(t *testing.T) {
	venue := newFakeVenue(t)
	executor, fake, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	const orders = 5
	mark := collector.Mark()
	var lastID godex.OrderID
	for range orders {
		ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentIOC))
		if err != nil {
			t.Fatalf("PlaceOrder: %v", err)
		}
		lastID = ack.OrderID
		params := fake.lastPlace(t)
		venueOrderID := "venue-order-" + strconv.FormatUint(uint64(params.clientID), 10)
		venue.push(orderFrame(t, params.clientID, venueOrderID, orderStatusOpen, nil))
		venue.push(orderFrame(t, params.clientID, venueOrderID, orderStatusFilled, nil))
	}

	// Cancelling the last one proves the stream has been drained: it only
	// reports unknown once the FILLED update has retired it.
	deadline := time.Now().Add(testEventTimeout)
	for !errors.Is(executor.CancelOrder(context.Background(), lastID), godex.ErrUnknownOrder) {
		if time.Now().After(deadline) {
			t.Fatal("a filled order was never retired")
		}
		time.Sleep(10 * time.Millisecond)
	}

	executor.stateMu.Lock()
	tracked, byClient := len(executor.orders), len(executor.orderIDsByClientID)
	byVenue := len(executor.orderIDsByVenueID)
	executor.stateMu.Unlock()
	if tracked != 0 || byClient != 0 {
		t.Fatalf("after %d filled orders: %d tracked, %d client-id entries; want none",
			orders, tracked, byClient)
	}
	// The venue-id mapping outlives the order on purpose (a late fill must stay
	// attributable), but it is bounded by age rather than growing forever.
	if byVenue > orders {
		t.Fatalf("venue-id mapping holds %d entries for %d orders", byVenue, orders)
	}

	for _, event := range collector.Events()[mark:] {
		if rejected, ok := event.(godex.OrderRejectedEvent); ok {
			t.Fatalf("a filled order was reported as rejected: %+v", rejected)
		}
	}
}

// TestVenueOrderMappingIsPrunedByAge keeps the late-attribution window from
// becoming an unbounded map in a long-running process.
func TestVenueOrderMappingIsPrunedByAge(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	ack, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly))
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	executor.stateMu.Lock()
	executor.rememberVenueOrderLocked("venue-order-stale", ack.OrderID)
	// Age the entry past the window in which a fill could still reference it.
	stale := executor.orderIDsByVenueID["venue-order-stale"]
	stale.at = stale.at.Add(-2 * venueOrderMappingTTL)
	executor.orderIDsByVenueID["venue-order-stale"] = stale
	executor.rememberVenueOrderLocked("venue-order-fresh", ack.OrderID)
	_, stalePresent := executor.orderIDsByVenueID["venue-order-stale"]
	_, freshPresent := executor.orderIDsByVenueID["venue-order-fresh"]
	executor.stateMu.Unlock()

	if stalePresent {
		t.Fatal("an expired venue-id mapping survived")
	}
	if !freshPresent {
		t.Fatal("pruning dropped a live venue-id mapping")
	}
}

// TestFillInAnotherMarketIsNotReportedAsOurs: one subaccount can trade several
// markets, and FillEvent carries no market of its own — so a foreign fill must
// be dropped rather than presented under this executor's symbol.
func TestFillInAnotherMarketIsNotReportedAsOurs(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	mark := collector.Mark()
	foreign := streamFillObject("fill-btc", "venue-order-btc", "65000", "0.010")
	foreign["ticker"] = "BTC-USD"
	frame, err := json.Marshal(map[string]any{
		"type":     wsTypeChannelData,
		"channel":  subaccountsChannel,
		"contents": map[string]any{"fills": []any{foreign}},
	})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	venue.push(frame)

	// Follow it with one of ours: when that arrives, the foreign fill has been
	// processed and demonstrably not emitted.
	venue.push(fillFrame(t, "fill-eth", "venue-order-eth", "3010.2", "0.100"))
	if _, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"own fill", isFillEvent); err != nil {
		t.Fatal(err)
	}
	for _, event := range collector.Events()[mark:] {
		if fill, ok := event.(godex.FillEvent); ok && fill.Size.String() == "0.010" {
			t.Fatalf("a BTC-USD fill was reported on the ETH-PERP stream: %+v", fill)
		}
	}
}

// positionFrame builds one channel_data position update for this executor's
// market.
func positionFrame(t *testing.T, size, entryPrice, unrealizedPnl string) []byte {
	t.Helper()
	frame, err := json.Marshal(map[string]any{
		"type":    wsTypeChannelData,
		"channel": subaccountsChannel,
		"contents": map[string]any{
			"perpetualPositions": []any{map[string]any{
				"market":        testTicker,
				"status":        "OPEN",
				"side":          "LONG",
				"size":          size,
				"entryPrice":    entryPrice,
				"unrealizedPnl": unrealizedPnl,
			}},
		},
	})
	if err != nil {
		t.Fatalf("encode position frame: %v", err)
	}
	return frame
}

// subaccountWithPosition rewrites the REST fixture's open position, so a test
// can say what a snapshot re-read should find.
func subaccountWithPosition(t *testing.T, size, entryPrice, unrealizedPnl string) string {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(loadFixture(t, "subaccount_rest.json"), &snapshot); err != nil {
		t.Fatalf("subaccount fixture decode: %v", err)
	}
	positions := snapshot["subaccount"].(map[string]any)["openPerpetualPositions"].(map[string]any)
	position := positions[testTicker].(map[string]any)
	position["size"] = size
	position["entryPrice"] = entryPrice
	position["unrealizedPnl"] = unrealizedPnl
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode subaccount: %v", err)
	}
	return string(body)
}

// TestZeroEntryPriceIsRereadNotEmitted: in the moment after a fill the stream
// reports the new size with an entry price of zero, and an unrealized PnL
// computed against that zero, before correcting both in the next update
// (testnet, 2026-07-27). A consumer measuring liquidation distance off entry
// price would read that as a position it does not hold, so the adapter re-reads
// the snapshot instead of publishing it.
func TestZeroEntryPriceIsRereadNotEmitted(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// What the re-read finds: the position the venue settles on.
	venue.setSubaccount(subaccountWithPosition(t, "0.002", "1954.9", "-0.028215654"))

	mark := collector.Mark()
	venue.push(positionFrame(t, "0.002", "0", "3.881584346"))

	// The first position after the transient is the one that matters: had the
	// transient been emitted, it would be this event.
	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"position", isPositionEvent)
	if err != nil {
		t.Fatal(err)
	}
	position := event.(godex.PositionEvent).Position
	if position.EntryPrice.String() != "1954.9" {
		t.Fatalf("entry price = %s, want the re-read 1954.9", position.EntryPrice)
	}
	if position.UnrealizedPnL.String() != "-0.028215654" {
		t.Fatalf("unrealized pnl = %s, want the re-read -0.028215654", position.UnrealizedPnL)
	}
	if position.Size.String() != "0.002" {
		t.Fatalf("position size = %s, want 0.002", position.Size)
	}
}

// TestZeroEntryPriceSurvivingIntoTheSnapshotIsRereadAgain: REST is served from
// the same Indexer state as the stream, so the re-read can land before the
// venue has priced the position. Taking that first answer would publish the
// exact number the re-read exists to avoid, so the snapshot is read again until
// it settles.
func TestZeroEntryPriceSurvivingIntoTheSnapshotIsRereadAgain(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// The first re-read still carries the transient; the second has settled.
	venue.setSubaccount(subaccountWithPosition(t, "0.002", "1954.9", "-0.028215654"))
	venue.queueSubaccount(subaccountWithPosition(t, "0.002", "0", "3.881584346"))

	mark := collector.Mark()
	venue.push(positionFrame(t, "0.002", "0", "3.881584346"))

	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"position", isPositionEvent)
	if err != nil {
		t.Fatal(err)
	}
	position := event.(godex.PositionEvent).Position
	if position.EntryPrice.String() != "1954.9" {
		t.Fatalf("entry price = %s, want the settled 1954.9", position.EntryPrice)
	}
	if position.UnrealizedPnL.String() != "-0.028215654" {
		t.Fatalf("unrealized pnl = %s, want the settled -0.028215654", position.UnrealizedPnL)
	}
}

// TestZeroEntryPriceThatIsRealIsStillPublished: refusing the transient must not
// become a way to suppress a position outright. A zero that repeats across
// reads is the venue's answer rather than a moment in flight, so it is
// published — after a bounded number of reads, not indefinitely many.
func TestZeroEntryPriceThatIsRealIsStillPublished(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, collector := newTestExecutor(t, venue)
	mustConnect(t, executor)

	venue.setSubaccount(subaccountWithPosition(t, "0.002", "0", "3.881584346"))

	mark := collector.Mark()
	before := venue.subaccountReadCount()
	venue.push(positionFrame(t, "0.002", "0", "3.881584346"))

	event, err := collector.WaitFor(context.Background(), mark, testEventTimeout,
		"position", isPositionEvent)
	if err != nil {
		t.Fatal(err)
	}
	position := event.(godex.PositionEvent).Position
	if position.Size.String() != "0.002" || !position.EntryPrice.IsZero() {
		t.Fatalf("position = size %s at %s, want 0.002 at 0", position.Size, position.EntryPrice)
	}
	if reads := venue.subaccountReadCount() - before; reads != positionPriceReads {
		t.Fatalf("%d snapshot reads, want %d: the confirmation must be bounded",
			reads, positionPriceReads)
	}
}

// TestSnapshotRereadIsSequencedBeforeLaterStreamUpdates: the re-read runs on its
// own goroutine, but its observation sequence has to be reserved before the
// stream reader moves on. Reserving it inside the goroutine leaves the ordering
// to the scheduler, and losing that race lets a REST response describing an
// older state outrank — and overwrite — the corrected update the reader handled
// in the meantime.
func TestSnapshotRereadIsSequencedBeforeLaterStreamUpdates(t *testing.T) {
	venue := newFakeVenue(t)
	executor, _, _ := newTestExecutor(t, venue)
	mustConnect(t, executor)

	// Hold the response so the re-read cannot finish and reserve late by luck.
	release := venue.blockSubaccount()
	defer release()

	executor.stateMu.Lock()
	before := executor.observationSeq
	executor.stateMu.Unlock()

	executor.requestSnapshotRefresh()

	executor.stateMu.Lock()
	after := executor.observationSeq
	executor.stateMu.Unlock()
	if after <= before {
		t.Fatalf("observation sequence %d unchanged from %d: the re-read must be "+
			"sequenced before requestSnapshotRefresh returns, so an update handled "+
			"afterwards outranks it", after, before)
	}
}

// TestCloseDuringConnectDoesNotSendOnAClosedChannel: Close and a Connect that
// is still waiting on the venue must not race. Without Connect being tracked as
// an operation, Close would close the events channel underneath it and the
// stream's first event would panic.
func TestCloseDuringConnectDoesNotSendOnAClosedChannel(t *testing.T) {
	venue := newFakeVenue(t)
	release := venue.blockMarkets()
	defer release()

	fake := &recordingSigner{addr: testAddress}
	executor, err := New(Config{
		Credentials: Credentials{PrivateKeyHex: testPrivateKeyHex, Address: testAddress},
		Symbol:      testSymbol,
		Ticker:      testTicker,
		Network:     Testnet,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),

		IndexerRESTBaseURL:   venue.server.URL + "/v4",
		IndexerWSURL:         venue.wsURL,
		RPCBaseURL:           venue.server.URL,
		TxRequestTimeout:     500 * time.Millisecond,
		TxFaultRecoveryDelay: 100 * time.Millisecond,
		HeightPollInterval:   50 * time.Millisecond,
		HeightStaleAfter:     2 * time.Second,
		newSigner:            func(*resolvedConfig) (signer, error) { return fake, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range executor.AccountEvents() {
		}
	}()

	connected := make(chan error, 1)
	go func() {
		_, err := executor.Connect(context.Background())
		connected <- err
	}()

	// Let Connect reach the blocked request, then close underneath it.
	time.Sleep(50 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- executor.Close() }()

	// Close must interrupt the in-flight Connect rather than wait on it forever.
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(testEventTimeout):
		t.Fatal("Close blocked on an in-flight Connect")
	}
	select {
	case err := <-connected:
		if err == nil {
			t.Fatal("Connect must not report success after Close")
		}
	case <-time.After(testEventTimeout):
		t.Fatal("Connect did not return after Close")
	}
	release()

	select {
	case <-drained:
	case <-time.After(testEventTimeout):
		t.Fatal("the events channel was never closed")
	}

	// A Connect that got further before Close ran is stopped by the same lock:
	// neither the stream nor the pollers may start once the executor is
	// terminal, since nothing would be waiting for them.
	if err := executor.installSocket(nil); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("installSocket after Close = %v, want ErrClosed", err)
	}
	if err := executor.startPollers(); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("startPollers after Close = %v, want ErrClosed", err)
	}
}

func TestCloseEmitsDisconnectedThenClosesChannel(t *testing.T) {
	venue := newFakeVenue(t)
	fake := &recordingSigner{addr: testAddress}
	executor, err := New(Config{
		Credentials: Credentials{
			PrivateKeyHex: testPrivateKeyHex,
			Address:       testAddress,
		},
		Symbol:               testSymbol,
		Ticker:               testTicker,
		Network:              Testnet,
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		IndexerRESTBaseURL:   venue.server.URL + "/v4",
		IndexerWSURL:         venue.wsURL,
		RPCBaseURL:           venue.server.URL,
		TxRequestTimeout:     500 * time.Millisecond,
		TxFaultRecoveryDelay: 100 * time.Millisecond,
		HeightPollInterval:   50 * time.Millisecond,
		HeightStaleAfter:     2 * time.Second,
		newSigner:            func(*resolvedConfig) (signer, error) { return fake, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := executor.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is terminal and idempotent.
	if err := executor.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	var events []godex.AccountEvent
	for event := range executor.AccountEvents() {
		events = append(events, event)
	}
	if len(events) == 0 {
		t.Fatal("expected events before the channel closed")
	}
	if _, ok := events[len(events)-1].(godex.DisconnectedEvent); !ok {
		t.Fatalf("last event is %T, want DisconnectedEvent", events[len(events)-1])
	}
	if _, err := executor.PlaceOrder(context.Background(), testOrder(godex.IntentPostOnly)); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("PlaceOrder after Close = %v, want ErrClosed", err)
	}
}
