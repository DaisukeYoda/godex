package txflow

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/smoketest"
	"github.com/gorilla/websocket"
)

const (
	testEventTimeout = 3 * time.Second
	testSymbol       = godex.Symbol("ETH-PERP")
	testMarket       = "ETH-USDC"
	testAccount      = "0x1719884eb866cb12b2287399b15f7db5e7d775ea"
	testPrivateKey   = "0x0123456789012345678901234567890123456789012345678901234567890123"
	// ETH-USDC carries index 2 in the fixture universe, at array position
	// 1, so a test that used the position would fail.
	testAssetIndex = 2
	// testMarginFraction is the caller-supplied maintenance rate.
	testMarginFractionText = "0.025"
	// firstFakeOid is the oid the fake venue assigns to the first order it
	// accepts; later ones count up from it.
	firstFakeOid int64 = 215841473334
	// firstFakeTid is where the fake's trade ids start.
	firstFakeTid int64 = 235744331000001
	// historyTid is a trade id the account's history holds before the
	// executor connects.
	historyTid int64 = 100
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

// scriptedExchange is one queued /exchange outcome. An order action is
// booked into the venue's records whatever the scripted answer, the way a
// venue that took the order but whose response was lost would hold it;
// lost marks a submission the venue never took.
type scriptedExchange struct {
	body   string
	status int
	delay  time.Duration
	lost   bool
}

// fakeOrder is one order in the fake venue's records.
type fakeOrder struct {
	oid        int64
	symbol     string
	status     string
	timestamp  int64
	open       bool
	side       string
	limitPx    string
	origSz     string
	reduceOnly bool
}

// testOrderAttrs is what testOrder looks like once quantized, the attributes
// the fake renders for orders it books and plants.
var testOrderAttrs = fakeOrder{side: sideBid, limitPx: "2986.35", origSz: "0.5"}

// fakeFill is one execution in the fake venue's userFills answer.
type fakeFill struct {
	tid    int64
	oid    int64
	symbol string
	px     string
	sz     string
	side   string
}

// fakeVenue is an httptest-backed TxFlow: /info answers from fixtures and
// from an order table the tests edit, a scriptable /exchange, and a WebSocket
// the tests push frames into.
type fakeVenue struct {
	server *httptest.Server
	wsURL  string

	// writeMu serializes frames onto a connection: gorilla allows a single
	// concurrent writer.
	writeMu sync.Mutex

	mu              sync.Mutex
	clearinghouse   []byte
	leverageType    string
	orders          map[int64]*fakeOrder
	nextOid         int64
	fills           []fakeFill
	exchangeQueue   []scriptedExchange
	exchangeCalls   []exchangeRequest
	exchangeActions []json.RawMessage
	infoTypes       []string
	conns           []*websocket.Conn
	inbound         []string
}

func newFakeVenue(t *testing.T) *fakeVenue {
	t.Helper()
	venue := &fakeVenue{
		clearinghouse: loadFixture(t, "clearinghouse_flat.json"),
		leverageType:  leverageTypeCross,
		orders:        make(map[int64]*fakeOrder),
		nextOid:       firstFakeOid,
		// The account has a fill in its history before the executor
		// connects; it must be absorbed, not published.
		fills: []fakeFill{{tid: historyTid, oid: 1, symbol: testMarket, px: "2900.1", sz: "0.2", side: sideBid}},
	}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()

	mux.HandleFunc(infoPath, func(w http.ResponseWriter, r *http.Request) {
		var request infoRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("info request decode: %v", err)
			return
		}
		venue.mu.Lock()
		defer venue.mu.Unlock()
		venue.infoTypes = append(venue.infoTypes, request.Type)

		switch request.Type {
		case infoTypePerpMeta:
			_, _ = w.Write(loadFixture(t, "perp_meta.json"))
		case infoTypeClearinghouseState:
			_, _ = w.Write(venue.clearinghouse)
		case infoTypeActiveAssetData:
			if _, err := strconv.Atoi(request.Coin); err != nil {
				t.Errorf("activeAssetData coin = %q, want an asset index", request.Coin)
			}
			_, _ = fmt.Fprintf(w, `{"user":%q,"coin":%q,"leverage":{"mode":"oneWay","type":%q,"value":10,"raw_usd":null},`+
				`"maxTradeSzs":["0.0000"],"availableToTrade":["0.000000"],"markPx":"0.0","onlyIsolated":false}`,
				testAccount, request.Coin, venue.leverageType)
		case infoTypeOpenOrders:
			_, _ = w.Write(venue.openOrdersBody())
		case infoTypeHistoricalOrders:
			_, _ = w.Write(venue.historicalOrdersBody())
		case infoTypeUserFills:
			_, _ = w.Write(venue.userFillsBody())
		default:
			// The venue allowlists its query types; anything the adapter is
			// not known to be allowed is refused the way the venue refuses.
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"This action is not allowed."}`))
		}
	})

	mux.HandleFunc(exchangePath, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("exchange body: %v", err)
			return
		}
		var request exchangeRequest
		var raw struct {
			Action json.RawMessage `json:"action"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("exchange decode: %v", err)
		}
		_ = json.Unmarshal(body, &raw)

		venue.mu.Lock()
		venue.exchangeCalls = append(venue.exchangeCalls, request)
		venue.exchangeActions = append(venue.exchangeActions, raw.Action)
		var script scriptedExchange
		if len(venue.exchangeQueue) > 0 {
			script = venue.exchangeQueue[0]
			venue.exchangeQueue = venue.exchangeQueue[1:]
		}
		var oid int64
		if !script.lost {
			oid = venue.bookActionLocked(t, raw.Action)
		}
		if script.body == "" {
			script.body = defaultExchangeBody(raw.Action, oid)
		}
		venue.mu.Unlock()

		if script.delay > 0 {
			time.Sleep(script.delay)
		}
		if script.status != 0 && script.status != http.StatusOK {
			w.WriteHeader(script.status)
		}
		_, _ = w.Write([]byte(script.body))
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
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
			if string(data) == wsMethodPing {
				_ = venue.write(conn, []byte(`{"method":"PONG"}`))
			}
		}
	})

	venue.server = httptest.NewServer(mux)
	venue.wsURL = "ws" + strings.TrimPrefix(venue.server.URL, "http") + "/ws"
	t.Cleanup(venue.server.Close)
	return venue
}

// bookActionLocked records an order action in the venue's records under a
// fresh oid, which it returns; other actions book nothing. Callers hold mu.
func (v *fakeVenue) bookActionLocked(t *testing.T, action json.RawMessage) int64 {
	t.Helper()
	var decoded struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(action, &decoded)
	switch decoded.Type {
	case actionTypeCancel:
		return 0
	case actionTypeOrder:
		var decoded orderAction
		if err := json.Unmarshal(action, &decoded); err != nil || len(decoded.Orders) != 1 {
			t.Errorf("order action is malformed: %v", err)
			return 0
		}
		wire := decoded.Orders[0]
		side := sideAsk
		if wire.IsBuy {
			side = sideBid
		}
		oid := v.nextOid
		v.nextOid++
		v.orders[oid] = &fakeOrder{
			oid: oid, symbol: testMarket, status: orderStatusOpen,
			timestamp: time.Now().UnixMilli(), open: true,
			side: side, limitPx: wire.Price, origSz: wire.Size, reduceOnly: wire.ReduceOnly,
		}
		return oid
	default:
		t.Errorf("unexpected exchange action type %q", decoded.Type)
		return 0
	}
}

// defaultExchangeBody is the venue's success answer for an action no case has
// scripted a body for: an order joins the book under its oid, and a cancel is
// accepted.
func defaultExchangeBody(action json.RawMessage, oid int64) string {
	var decoded struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(action, &decoded)
	if decoded.Type == actionTypeCancel {
		return `{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}`
	}
	return fmt.Sprintf(`{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":%d}}]}}}`, oid)
}

func (v *fakeVenue) openOrdersBody() []byte {
	entries := make([]string, 0)
	for _, order := range v.sortedOrders() {
		if !order.open {
			continue
		}
		entries = append(entries, fmt.Sprintf(`{"coin":%q,"limitPx":"2986.35","oid":%d,"side":"B","sz":"0.5","timestamp":%d,"cloid":null}`,
			order.symbol, order.oid, order.timestamp))
	}
	return []byte("[" + strings.Join(entries, ",") + "]")
}

func (v *fakeVenue) historicalOrdersBody() []byte {
	entries := make([]string, 0)
	for _, order := range v.sortedOrders() {
		entries = append(entries, fmt.Sprintf(`{"order":{"coin":"ETH","symbol":%q,"side":%q,"limitPx":%q,"sz":"0.0",`+
			`"oid":%d,"timestamp":%d,"reduceOnly":%t,"origSz":%q,"tif":"GTC","cloid":null,"status":%q,`+
			`"instrumentId":%d},"status":%q,"statusTimestamp":%d}`,
			order.symbol, order.side, order.limitPx, order.oid, order.timestamp, order.reduceOnly, order.origSz,
			order.status, testAssetIndex, lowerCamel(order.status), order.timestamp))
	}
	return []byte("[" + strings.Join(entries, ",") + "]")
}

// lowerCamel renders the top-level status the way the venue does — a
// spelling the adapter must ignore in favor of order.status.
func lowerCamel(status string) string {
	return strings.ToLower(status[:1]) + status[1:]
}

func (v *fakeVenue) userFillsBody() []byte {
	entries := make([]string, 0, len(v.fills))
	for _, fill := range v.fills {
		entries = append(entries, fmt.Sprintf(`{"coin":"ETH","symbol":%q,"px":%q,"sz":%q,"side":%q,"time":1787051571065,`+
			`"startPosition":"0.0","dir":"Buy","closedPnl":"0","hash":"0xabc","oid":%d,"crossed":null,"fee":"0.1",`+
			`"tid":%d,"feeToken":"USDC","tradeSide":"Maker","instrumentId":%d}`,
			fill.symbol, fill.px, fill.sz, fill.side, fill.oid, fill.tid, testAssetIndex))
	}
	return []byte("[" + strings.Join(entries, ",") + "]")
}

func (v *fakeVenue) sortedOrders() []*fakeOrder {
	orders := make([]*fakeOrder, 0, len(v.orders))
	for _, order := range v.orders {
		orders = append(orders, order)
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].oid < orders[j].oid })
	return orders
}

func (v *fakeVenue) queueExchange(scripts ...scriptedExchange) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.exchangeQueue = append(v.exchangeQueue, scripts...)
}

func (v *fakeVenue) exchangeCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.exchangeCalls)
}

// lastOrderWire decodes the most recent order action's single order.
func (v *fakeVenue) lastOrderWire(t *testing.T) orderWire {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.exchangeActions) == 0 {
		t.Fatal("no exchange submission recorded")
	}
	var action orderAction
	if err := json.Unmarshal(v.exchangeActions[len(v.exchangeActions)-1], &action); err != nil {
		t.Fatalf("decode order action: %v", err)
	}
	if action.Type != actionTypeOrder || len(action.Orders) != 1 {
		t.Fatalf("unexpected order action: %+v", action)
	}
	if action.Grouping != groupingNA {
		t.Errorf("grouping = %q, want %q", action.Grouping, groupingNA)
	}
	return action.Orders[0]
}

func (v *fakeVenue) lastCancelWire(t *testing.T) cancelWire {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	var action cancelAction
	if err := json.Unmarshal(v.exchangeActions[len(v.exchangeActions)-1], &action); err != nil {
		t.Fatalf("decode cancel action: %v", err)
	}
	if action.Type != actionTypeCancel || len(action.Cancels) != 1 {
		t.Fatalf("unexpected cancel action: %+v", action)
	}
	return action.Cancels[0]
}

// exchangeActionTypes reports the "type" of every action submitted so far.
func (v *fakeVenue) exchangeActionTypes(t *testing.T) []string {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	types := make([]string, 0, len(v.exchangeActions))
	for _, raw := range v.exchangeActions {
		var action struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &action); err != nil {
			t.Fatalf("decode action: %v", err)
		}
		types = append(types, action.Type)
	}
	return types
}

func (v *fakeVenue) nonces() []uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	nonces := make([]uint64, 0, len(v.exchangeCalls))
	for _, call := range v.exchangeCalls {
		nonces = append(nonces, call.Nonce)
	}
	return nonces
}

func (v *fakeVenue) setClearinghouse(body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clearinghouse = []byte(body)
}

func (v *fakeVenue) setLeverageType(leverage string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.leverageType = leverage
}

// setOrderState rewrites what the venue's records say about oid: its
// lifecycle status and whether it still rests on the book.
func (v *fakeVenue) setOrderState(t *testing.T, oid int64, status string, open bool) {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	order, known := v.orders[oid]
	if !known {
		t.Fatalf("setOrderState: the fake venue holds no oid %d", oid)
	}
	order.status, order.open = status, open
}

// forgetOrder drops oid from the venue's records entirely.
func (v *fakeVenue) forgetOrder(oid int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.orders, oid)
}

// addOrder plants an order the executor did not place — the way another
// process on the account leaves one. attrs supplies side, price, size and
// reduce-only; symbol, status, open and timestamp are taken from the
// arguments.
func (v *fakeVenue) addOrder(symbol, status string, open bool, timestamp int64, attrs fakeOrder) int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	oid := v.nextOid
	v.nextOid++
	attrs.oid, attrs.symbol, attrs.status, attrs.open, attrs.timestamp = oid, symbol, status, open, timestamp
	v.orders[oid] = &attrs
	return oid
}

// addFill appends an execution against oid to the account's fills; the
// executor's poll is what publishes it.
func (v *fakeVenue) addFill(oid, tid int64) {
	v.addFillOn(testMarket, oid, tid)
}

func (v *fakeVenue) addFillOn(symbol string, oid, tid int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.fills = append(v.fills, fakeFill{tid: tid, oid: oid, symbol: symbol, px: "2986.3", sz: "0.5", side: sideBid})
}

// setFillFields overrides the price/size text of every fill, for cases about
// malformed payloads.
func (v *fakeVenue) setFillFields(px, sz string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for i := range v.fills {
		v.fills[i].px, v.fills[i].sz = px, sz
	}
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
	if err := v.write(conn, frame); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func (v *fakeVenue) subscriptions() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	sent := make([]string, len(v.inbound))
	copy(sent, v.inbound)
	return sent
}

func (v *fakeVenue) infoRequests() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	types := make([]string, len(v.infoTypes))
	copy(types, v.infoTypes)
	return types
}

func (v *fakeVenue) write(conn *websocket.Conn, frame []byte) error {
	v.writeMu.Lock()
	defer v.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, frame)
}

// recordingSigner wraps the real signer so tests can assert on what was
// signed while keeping the production signing path under test.
type recordingSigner struct {
	inner signer

	mu      sync.Mutex
	actions []any
	nonces  []uint64
}

func (s *recordingSigner) signAction(action any, nonce uint64) (signature, error) {
	s.mu.Lock()
	s.actions = append(s.actions, action)
	s.nonces = append(s.nonces, nonce)
	s.mu.Unlock()
	return s.inner.signAction(action, nonce)
}

func (s *recordingSigner) address() string { return s.inner.address() }

func newTestExecutor(t *testing.T, venue *fakeVenue) (*Executor, *smoketest.Collector) {
	t.Helper()
	executor, collector, _ := newTestExecutorStream(t, venue)
	return executor, collector
}

// newTestExecutorStream is newTestExecutor, also reporting when the account
// event channel has closed and the collector therefore holds the whole stream.
// Close returning is not that moment; the conformance suite needs the one that
// is.
func newTestExecutorStream(t *testing.T, venue *fakeVenue) (*Executor, *smoketest.Collector, <-chan struct{}) {
	t.Helper()
	executor, err := New(testConfig(venue))
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
	return executor, collector, consumed
}

func testConfig(venue *fakeVenue) Config {
	return Config{
		Credentials: Credentials{
			AccountAddress: testAccount,
			APIPrivateKey:  testPrivateKey,
		},
		Symbol:                    testSymbol,
		Market:                    testMarket,
		Network:                   Mainnet,
		MaintenanceMarginFraction: decimal.MustFromString(testMarginFractionText, 3),
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
		TxFaultRecoveryDelay: 50 * time.Millisecond,
		AccountPollInterval:  time.Hour, // tests drive refreshes explicitly
		FillPollInterval:     20 * time.Millisecond,
		newSigner: func(cfg *resolvedConfig) (signer, error) {
			inner, err := newKeySigner(cfg.credentials.APIPrivateKey, cfg.signing)
			if err != nil {
				return nil, err
			}
			return &recordingSigner{inner: inner}, nil
		},
	}
}

func mustConnect(t *testing.T, executor *Executor) godex.ExecutionMetadata {
	t.Helper()
	metadata, err := executor.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return metadata
}

// oidOf reports the venue oid the executor bound to one of its orders, whether
// or not it still tracks it.
func (e *Executor) oidOf(t *testing.T, id godex.OrderID) int64 {
	t.Helper()
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if oid, tracked := e.orders[id]; tracked && oid != 0 {
		return oid
	}
	for oid, bound := range e.oids.byOid {
		if bound == id {
			return oid
		}
	}
	t.Fatalf("order %s has no bound oid", id)
	return 0
}

func testOrder(intent godex.OrderIntent) godex.NewOrder {
	return godex.NewOrder{
		Symbol: testSymbol,
		Side:   godex.SideBuy,
		Price:  decimal.MustFromString("2986.3567", 4),
		Size:   decimal.MustFromString("0.50005", 5),
		Intent: intent,
	}
}

func orderUpdateFrame(oid int64, status string) []byte {
	return fmt.Appendf(nil, `{"channel":"orderUpdates","data":[{"order":{"coin":"ETH","symbol":"ETH-USDC","side":"B",`+
		`"limitPx":"2986.35","sz":"0.0","oid":%d,"timestamp":1753660000000,"origSz":"0.5",`+
		`"cloid":null},"status":%q,"statusTimestamp":1753660001000}]}`, oid, status)
}

func isPositionEvent(e godex.AccountEvent) bool { _, ok := e.(godex.PositionEvent); return ok }
func isMarginEvent(e godex.AccountEvent) bool   { _, ok := e.(godex.MarginEvent); return ok }
func isFillEvent(e godex.AccountEvent) bool     { _, ok := e.(godex.FillEvent); return ok }
func isConnectedEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.ConnectedEvent)
	return ok
}

func isDisconnectedEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.DisconnectedEvent)
	return ok
}

func isRejectionEvent(e godex.AccountEvent) bool {
	_, ok := e.(godex.OrderRejectedEvent)
	return ok
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

func countRejectionsFor(events []godex.AccountEvent, id godex.OrderID) int {
	count := 0
	for _, event := range events {
		if rejection, ok := event.(godex.OrderRejectedEvent); ok && rejection.OrderID == id {
			count++
		}
	}
	return count
}

// lastCancelWireAt decodes the cancel action at position index.
func (v *fakeVenue) lastCancelWireAt(t *testing.T, index int) cancelWire {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	var action cancelAction
	if err := json.Unmarshal(v.exchangeActions[index], &action); err != nil {
		t.Fatalf("decode cancel action: %v", err)
	}
	if action.Type != actionTypeCancel || len(action.Cancels) != 1 {
		t.Fatalf("unexpected cancel action: %+v", action)
	}
	return action.Cancels[0]
}
