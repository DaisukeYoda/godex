package hyperliquid

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	testCoin         = "ETH"
	testAccount      = "0x1719884eb866cb12b2287399b15f7db5e7d775ea"
	// ETH sits at index 4 in the fixture universe, so a test that silently
	// used the first entry would fail.
	testAssetIndex = 4
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

// scriptedExchange is one queued /exchange outcome.
type scriptedExchange struct {
	body   string
	status int
	delay  time.Duration
}

// fakeVenue is an httptest-backed Hyperliquid: /info answers from fixtures, a
// scriptable /exchange, and a WebSocket the tests push frames into.
type fakeVenue struct {
	server *httptest.Server
	wsURL  string

	// writeMu serializes frames onto a connection: gorilla allows a single
	// concurrent writer, and the subscription handshake and a test's push can
	// otherwise write at once.
	writeMu sync.Mutex

	mu               sync.Mutex
	clearinghouse    []byte
	orderQueryStatus string
	orderQueryInner  string
	leverageType     string
	snapshotFills    []int64
	suppressSnapshot bool
	exchangeQueue    []scriptedExchange
	exchangeCalls    []exchangeRequest
	exchangeActions  []json.RawMessage
	infoTypes        []string
	conns            []*websocket.Conn
	inbound          []string
}

func newFakeVenue(t *testing.T) *fakeVenue {
	t.Helper()
	venue := &fakeVenue{
		clearinghouse:    loadFixture(t, "clearinghouse_flat.json"),
		orderQueryStatus: queryStatusUnknownOid,
		leverageType:     leverageTypeCross,
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
		venue.infoTypes = append(venue.infoTypes, request.Type)
		clearinghouse, queryStatus := venue.clearinghouse, venue.orderQueryStatus
		queryInner, leverage := venue.orderQueryInner, venue.leverageType
		venue.mu.Unlock()

		switch request.Type {
		case infoTypeMeta:
			_, _ = w.Write(loadFixture(t, "meta.json"))
		case infoTypeClearinghouseState:
			_, _ = w.Write(clearinghouse)
		case infoTypeOrderStatus:
			body := `{"status":"` + queryStatus + `"}`
			if queryStatus == queryStatusOrder && queryInner != "" {
				body = `{"status":"order","order":{"status":"` + queryInner + `"}}`
			}
			_, _ = w.Write([]byte(body))
		case infoTypeActiveAssetData:
			_, _ = fmt.Fprintf(w, `{"user":%q,"coin":%q,"leverage":{"type":%q,"value":20}}`,
				testAccount, testCoin, leverage)
		case infoTypeExtraAgents:
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Errorf("unexpected info request type %q", request.Type)
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
		script := scriptedExchange{body: `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77738308}}]}}}`}
		if len(venue.exchangeQueue) > 0 {
			script = venue.exchangeQueue[0]
			venue.exchangeQueue = venue.exchangeQueue[1:]
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
			snapshotFills, suppress := venue.snapshotFills, venue.suppressSnapshot
			venue.mu.Unlock()
			venue.answerSubscription(t, conn, data, snapshotFills, suppress)
		}
	})

	venue.server = httptest.NewServer(mux)
	venue.wsURL = "ws" + strings.TrimPrefix(venue.server.URL, "http") + "/ws"
	t.Cleanup(venue.server.Close)
	return venue
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
	var action struct {
		Type     string      `json:"type"`
		Orders   []orderWire `json:"orders"`
		Grouping string      `json:"grouping"`
	}
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

func (v *fakeVenue) lastCancelWire(t *testing.T) cancelByCloidWire {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	var action struct {
		Type    string              `json:"type"`
		Cancels []cancelByCloidWire `json:"cancels"`
	}
	if err := json.Unmarshal(v.exchangeActions[len(v.exchangeActions)-1], &action); err != nil {
		t.Fatalf("decode cancel action: %v", err)
	}
	if action.Type != actionTypeCancelByCloid || len(action.Cancels) != 1 {
		t.Fatalf("unexpected cancel action: %+v", action)
	}
	return action.Cancels[0]
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

func (v *fakeVenue) setOrderQueryStatus(status string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.orderQueryStatus = status
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
	executor, err := New(Config{
		Credentials: Credentials{
			AccountAddress: testAccount,
			APIPrivateKey:  referencePrivateKey,
		},
		Symbol:  testSymbol,
		Coin:    testCoin,
		Network: Testnet,
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
		FillSnapshotTimeout:  2 * time.Second,
		AccountPollInterval:  time.Hour, // tests drive refreshes explicitly
		newSigner: func(cfg *resolvedConfig) (signer, error) {
			inner, err := newKeySigner(cfg.credentials.APIPrivateKey, cfg.signingSource, cfg.vaultAddress)
			if err != nil {
				return nil, err
			}
			return &recordingSigner{inner: inner}, nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	collector := smoketest.NewCollector(t.Logf)
	// Registered before the close cleanup so it runs after it (cleanups are
	// LIFO): the stream is complete only once the channel has been drained.
	t.Cleanup(func() {
		if err := smoketest.CheckContract(collector.Events()); err != nil {
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
	return executor, collector
}

func mustConnect(t *testing.T, executor *Executor) godex.ExecutionMetadata {
	t.Helper()
	metadata, err := executor.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return metadata
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

// answerSubscription mirrors the venue's own handshake: it acknowledges the
// subscription and, for userFills, follows with the opening snapshot Connect
// waits on.
func (v *fakeVenue) answerSubscription(t *testing.T, conn *websocket.Conn, data []byte, fills []int64, suppress bool) {
	t.Helper()
	var request struct {
		Method       string            `json:"method"`
		Subscription map[string]string `json:"subscription"`
	}
	if err := json.Unmarshal(data, &request); err != nil || request.Method != "subscribe" {
		return
	}
	_ = v.write(conn, []byte(`{"channel":"subscriptionResponse","data":{"method":"subscribe"}}`))
	if request.Subscription["type"] != channelUserFills || suppress {
		return
	}
	_ = v.write(conn, fillFrame(t, true, fills...))
}

func (v *fakeVenue) setSnapshotFills(tradeIDs ...int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snapshotFills = tradeIDs
}

func (v *fakeVenue) setSuppressSnapshot(suppress bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.suppressSnapshot = suppress
}

func (v *fakeVenue) setLeverageType(leverage string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.leverageType = leverage
}

// setOrderQuery scripts the orderStatus answer: the outer status and, when the
// venue holds the order, its lifecycle status.
func (v *fakeVenue) setOrderQuery(status, innerStatus string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.orderQueryStatus, v.orderQueryInner = status, innerStatus
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

func (v *fakeVenue) write(conn *websocket.Conn, frame []byte) error {
	v.writeMu.Lock()
	defer v.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, frame)
}
