package lighter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/gorilla/websocket"
)

// The scenarios port the reference implementation's connector tests (omnibook
// lighter-connector.test.ts) onto the Go MarketStream: snapshot+update
// reassembly, nonce-gap teardown, crossed-book suppression, and keepalive.

// fakeMarketVenue is an httptest-backed public order_book endpoint the tests
// push frames into.
type fakeMarketVenue struct {
	t      *testing.T
	server *httptest.Server
	wsURL  string

	mu      sync.Mutex
	conns   []*websocket.Conn
	inbound []string
}

func newFakeMarketVenue(t *testing.T) *fakeMarketVenue {
	t.Helper()
	venue := &fakeMarketVenue{t: t}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
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

func (v *fakeMarketVenue) connCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

// waitConn waits until connection index exists and returns it.
func (v *fakeMarketVenue) waitConn(t *testing.T, index int) *websocket.Conn {
	t.Helper()
	waitForCondition(t, fmt.Sprintf("connection %d", index), func() bool {
		return v.connCount() > index
	})
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.conns[index]
}

func (v *fakeMarketVenue) push(t *testing.T, conn *websocket.Conn, frame string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func (v *fakeMarketVenue) inboundMessages() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.inbound...)
}

func (v *fakeMarketVenue) countInbound(message string) int {
	count := 0
	for _, received := range v.inboundMessages() {
		if received == message {
			count++
		}
	}
	return count
}

// marketCollector consumes a stream's Events channel into an inspectable log.
type marketCollector struct {
	mu     sync.Mutex
	events []godex.MarketEvent
	done   chan struct{}
}

func collectMarketEvents(stream *MarketStream) *marketCollector {
	collector := &marketCollector{done: make(chan struct{})}
	go func() {
		for event := range stream.Events() {
			collector.mu.Lock()
			collector.events = append(collector.events, event)
			collector.mu.Unlock()
		}
		close(collector.done)
	}()
	return collector
}

func (c *marketCollector) snapshot() []godex.MarketEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]godex.MarketEvent(nil), c.events...)
}

func (c *marketCollector) books() []godex.OrderBook {
	var books []godex.OrderBook
	for _, event := range c.snapshot() {
		if book, ok := event.(godex.BookSnapshotEvent); ok {
			books = append(books, book.Book)
		}
	}
	return books
}

// connectionKinds returns the non-snapshot event sequence ("connected" /
// "disconnected") for asserting the ordering contract.
func (c *marketCollector) connectionKinds() []string {
	var kinds []string
	for _, event := range c.snapshot() {
		switch event.(type) {
		case godex.MarketConnectedEvent:
			kinds = append(kinds, "connected")
		case godex.MarketDisconnectedEvent:
			kinds = append(kinds, "disconnected")
		}
	}
	return kinds
}

func (c *marketCollector) waitBooks(t *testing.T, count int) []godex.OrderBook {
	t.Helper()
	waitForCondition(t, fmt.Sprintf("%d book snapshots", count), func() bool {
		return len(c.books()) >= count
	})
	return c.books()
}

func (c *marketCollector) waitDisconnected(t *testing.T) {
	t.Helper()
	waitForCondition(t, "disconnected event", func() bool {
		for _, kind := range c.connectionKinds() {
			if kind == "disconnected" {
				return true
			}
		}
		return false
	})
}

// waitConnectionKinds waits for the exact connected/disconnected sequence.
// Tests that abort a connection wait for the trailing "connected" of the
// automatic reconnect so teardown does not overlap an in-flight open (see
// TestMarketStreamCloseDuringReconnect).
func (c *marketCollector) waitConnectionKinds(t *testing.T, want ...string) {
	t.Helper()
	waitForCondition(t, fmt.Sprintf("connection sequence %v", want), func() bool {
		return fmt.Sprint(c.connectionKinds()) == fmt.Sprint(want)
	})
}

func waitForCondition(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(testEventTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// fakeMarketClock is the injected Now for deterministic crossed-book grace
// tests (no real sleeps).
type fakeMarketClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeMarketClock() *fakeMarketClock {
	return &fakeMarketClock{now: time.Unix(1781193600, 0)}
}

func (c *fakeMarketClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeMarketClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func testMarketReconnect() godex.ReconnectConfig {
	return godex.ReconnectConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2,
		IdleTimeout:  time.Second,
	}
}

// newTestMarketStream builds a BTC (market 1) stream against the fake venue;
// mutate adjusts the config before construction.
func newTestMarketStream(t *testing.T, venue *fakeMarketVenue, mutate func(*MarketStreamConfig)) (*MarketStream, *marketCollector) {
	t.Helper()
	cfg := MarketStreamConfig{
		Symbol:     "BTC-PERP",
		MarketID:   1,
		PriceScale: 1,
		SizeScale:  5,
		Reconnect:  testMarketReconnect(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		WSURL:      venue.wsURL,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	stream, err := NewMarketStream(cfg)
	if err != nil {
		t.Fatalf("NewMarketStream: %v", err)
	}
	collector := collectMarketEvents(stream)
	t.Cleanup(func() {
		_ = stream.Close()
		<-collector.done
	})
	return stream, collector
}

func marketLevel(price, size string) string {
	return fmt.Sprintf(`{"price":"%s","size":"%s"}`, price, size)
}

func marketLevels(entries ...string) string {
	return strings.Join(entries, ",")
}

func marketBookFrame(messageType, marketID, bids, asks string, nonce, beginNonce int64) string {
	return fmt.Sprintf(`{"type":"%s","channel":"order_book:%s","order_book":{"bids":[%s],"asks":[%s],"nonce":%d,"begin_nonce":%d}}`,
		messageType, marketID, bids, asks, nonce, beginNonce)
}

// btcSnapshotFrame mirrors SUBSCRIBED_BTC_ORDER_BOOK: bids intentionally
// unsorted to verify output sorting; nonce chain starts at 100.
func btcSnapshotFrame() string {
	return marketBookFrame(wsTypeSubscribedOrderBook, "1",
		marketLevels(
			marketLevel("63133.0", "0.03260"),
			marketLevel("63135.0", "0.03290"),
			marketLevel("63131.0", "0.63380"),
		),
		marketLevels(
			marketLevel("63136.0", "0.47240"),
			marketLevel("63140.0", "162.80970"),
		),
		100, 0)
}

const subscribeBTCMessage = `{"type":"subscribe","channel":"order_book/1"}`

// syncMarketStream starts the stream and drives it to connected + first
// snapshot (nonce 100), asserting the subscribe wire format on the way.
func syncMarketStream(t *testing.T, venue *fakeMarketVenue, stream *MarketStream, collector *marketCollector) *websocket.Conn {
	t.Helper()
	if err := stream.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn := venue.waitConn(t, 0)
	waitForCondition(t, "subscribe message", func() bool {
		return venue.countInbound(subscribeBTCMessage) >= 1
	})
	venue.push(t, conn, `{"type":"connected","session_id":"test-session"}`)
	venue.push(t, conn, btcSnapshotFrame())
	collector.waitBooks(t, 1)
	if kinds := collector.connectionKinds(); len(kinds) == 0 || kinds[0] != "connected" {
		t.Fatalf("expected a leading connected event, got %v", kinds)
	}
	return conn
}

func bidStrings(book godex.OrderBook) [][2]string {
	levels := make([][2]string, 0, len(book.Bids))
	for _, level := range book.Bids {
		levels = append(levels, [2]string{level.Price.String(), level.Size.String()})
	}
	return levels
}

func askPrices(book godex.OrderBook) []string {
	prices := make([]string, 0, len(book.Asks))
	for _, level := range book.Asks {
		prices = append(prices, level.Price.String())
	}
	return prices
}

func TestMarketStreamNormalizesSnapshotAndAppliesUpdates(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	first := collector.books()[0]
	if first.VenueID != godex.VenueLighter || first.Symbol != "BTC-PERP" {
		t.Fatalf("unexpected identity: %s %s", first.VenueID, first.Symbol)
	}
	// Bids arrive unsorted and come out best (highest) first.
	wantBids := [][2]string{{"63135.0", "0.03290"}, {"63133.0", "0.03260"}, {"63131.0", "0.63380"}}
	if got := bidStrings(first); fmt.Sprint(got) != fmt.Sprint(wantBids) {
		t.Fatalf("bids = %v, want %v", got, wantBids)
	}
	if got := first.Asks[0].Size.String(); got != "0.47240" {
		t.Fatalf("best ask size = %s", got)
	}

	// Delta: one level replaced, one removed (absolute size "0.00000" means
	// the level is absent).
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63135.0", "0.06510"), marketLevel("63131.0", "0.00000")),
		"", 107, 100))
	second := collector.waitBooks(t, 2)[1]
	wantBids = [][2]string{{"63135.0", "0.06510"}, {"63133.0", "0.03260"}}
	if got := bidStrings(second); fmt.Sprint(got) != fmt.Sprint(wantBids) {
		t.Fatalf("bids after update = %v, want %v", got, wantBids)
	}

	// Close ends with a final disconnected, then the channel closes.
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-collector.done
	events := collector.snapshot()
	if _, ok := events[len(events)-1].(godex.MarketDisconnectedEvent); !ok {
		t.Fatalf("last event = %T, want MarketDisconnectedEvent", events[len(events)-1])
	}
}

// One stream serves one market; independent streams keep independent nonce
// chains (BTC 100→..., ETH 500→...) without disturbing each other.
func TestMarketStreamsTrackNonceChainsIndependently(t *testing.T) {
	venue := newFakeMarketVenue(t)
	btcStream, btcCollector := newTestMarketStream(t, venue, nil)
	if err := btcStream.Start(context.Background()); err != nil {
		t.Fatalf("btc Start: %v", err)
	}
	btcConn := venue.waitConn(t, 0)

	ethStream, ethCollector := newTestMarketStream(t, venue, func(cfg *MarketStreamConfig) {
		cfg.Symbol = "ETH-PERP"
		cfg.MarketID = 0
		cfg.PriceScale = 2
		cfg.SizeScale = 4
	})
	if err := ethStream.Start(context.Background()); err != nil {
		t.Fatalf("eth Start: %v", err)
	}
	ethConn := venue.waitConn(t, 1)
	waitForCondition(t, "both subscribes", func() bool {
		return venue.countInbound(subscribeBTCMessage) >= 1 &&
			venue.countInbound(`{"type":"subscribe","channel":"order_book/0"}`) >= 1
	})

	venue.push(t, btcConn, `{"type":"connected"}`)
	venue.push(t, btcConn, btcSnapshotFrame())
	venue.push(t, ethConn, `{"type":"connected"}`)
	venue.push(t, ethConn, marketBookFrame(wsTypeSubscribedOrderBook, "0",
		marketLevels(marketLevel("1668.30", "10.0000")),
		marketLevels(marketLevel("1668.40", "5.0000")),
		500, 0))
	btcCollector.waitBooks(t, 1)
	ethCollector.waitBooks(t, 1)

	venue.push(t, ethConn, marketBookFrame(wsTypeUpdateOrderBook, "0",
		marketLevels(marketLevel("1668.30", "12.0000")), "", 503, 500))
	venue.push(t, btcConn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63135.0", "0.50000")), "", 101, 100))
	lastEth := ethCollector.waitBooks(t, 2)[1]
	btcCollector.waitBooks(t, 2)

	if got := lastEth.Bids[0].Size.String(); got != "12.0000" {
		t.Fatalf("eth best bid size = %s, want 12.0000", got)
	}
	for name, collector := range map[string]*marketCollector{"btc": btcCollector, "eth": ethCollector} {
		for _, kind := range collector.connectionKinds() {
			if kind == "disconnected" {
				t.Fatalf("%s stream disconnected unexpectedly", name)
			}
		}
	}
}

func TestMarketStreamNonceGapRebuildsConnectionAndBook(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	// begin_nonce 150 against lastNonce 100: continuity is lost, the
	// connection is aborted and automatically rebuilt.
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63135.0", "0.50000")), "", 151, 150))
	collector.waitDisconnected(t)

	newConn := venue.waitConn(t, 1)
	waitForCondition(t, "resubscribe", func() bool {
		return venue.countInbound(subscribeBTCMessage) >= 2
	})

	// The book is rebuilt from the fresh snapshot alone — nothing of the old
	// book survives.
	venue.push(t, newConn, `{"type":"connected"}`)
	venue.push(t, newConn, marketBookFrame(wsTypeSubscribedOrderBook, "1",
		marketLevels(marketLevel("63200.0", "1.50000")),
		marketLevels(marketLevel("63201.0", "2.00000")),
		200, 0))
	var fresh *godex.OrderBook
	waitForCondition(t, "fresh snapshot", func() bool {
		for _, book := range collector.books() {
			if len(book.Bids) > 0 && book.Bids[0].Price.String() == "63200.0" {
				fresh = &book
				return true
			}
		}
		return false
	})
	if len(fresh.Bids) != 1 || len(fresh.Asks) != 1 {
		t.Fatalf("rebuilt book kept stale levels: %+v", fresh)
	}
	want := []string{"connected", "disconnected", "connected"}
	if got := collector.connectionKinds(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("connection events = %v, want %v", got, want)
	}
}

func TestMarketStreamUpdateForUnsubscribedMarketRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "99", "", "", 1, 0))
	collector.waitDisconnected(t)
	venue.waitConn(t, 1)
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
}

// Close during an in-flight automatic reconnect: the reconnect dial goroutine
// runs handleOpen (which emits MarketConnectedEvent) without being tracked by
// the socket's WaitGroup, so MarketStream.Close's close(s.events) can race —
// and lose against — that emit. Close's comment claims socket.Stop makes this
// impossible, but Stop only waits for read loops and watchdogs, not the dial
// goroutine's OnOpen. Observed as a -race failure (send on s.events in
// handleOpen vs close in Close); worst case is a send-on-closed-channel panic.
func TestMarketStreamCloseDuringReconnect(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	// Abort the connection, then Close as soon as the server has accepted the
	// reconnect — the client-side OnOpen is still in flight.
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "99", "", "", 1, 0))
	collector.waitDisconnected(t)
	venue.waitConn(t, 1)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-collector.done
}

func TestMarketStreamCrossedBookSuppressesEmitsUntilResolved(t *testing.T) {
	venue := newFakeMarketVenue(t)
	// A grace far beyond the test's lifetime: only natural resolution may
	// resume emits here.
	stream, collector := newTestMarketStream(t, venue, func(cfg *MarketStreamConfig) {
		cfg.CrossedBookGrace = time.Hour
	})
	conn := syncMarketStream(t, venue, stream, collector)
	emittedBefore := len(collector.books())

	// A bid at the best ask (63136) crosses the book: emits are suppressed.
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63136.0", "0.50000")), "", 101, 100))
	// Updates keep applying to the book while crossed (still within grace).
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		"", marketLevels(marketLevel("63138.0", "2.00000")), 102, 101))
	// The crossing bid disappears: natural resolution, emits resume.
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63136.0", "0.00000")), "", 103, 102))

	books := collector.waitBooks(t, emittedBefore+1)
	// The crossed books (after nonces 101 and 102) were never emitted; only
	// the resolved book arrives.
	if len(books) != emittedBefore+1 {
		t.Fatalf("snapshots = %d, want %d", len(books), emittedBefore+1)
	}
	resumed := books[emittedBefore]
	if got := resumed.Bids[0].Price.String(); got != "63135.0" {
		t.Fatalf("best bid after resolution = %s, want 63135.0", got)
	}
	// The update applied while crossed (ask 63138) is present after resolution.
	found := false
	for _, price := range askPrices(resumed) {
		if price == "63138.0" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ask 63138.0 missing after resolution: %v", askPrices(resumed))
	}
	for _, kind := range collector.connectionKinds() {
		if kind == "disconnected" {
			t.Fatal("crossed book within grace must not disconnect")
		}
	}
	if venue.connCount() != 1 {
		t.Fatalf("connections = %d, want 1", venue.connCount())
	}
}

func TestMarketStreamCrossedBeyondGraceRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	clock := newFakeMarketClock()
	grace := 50 * time.Millisecond
	stream, collector := newTestMarketStream(t, venue, func(cfg *MarketStreamConfig) {
		cfg.CrossedBookGrace = grace
		cfg.Now = clock.Now
	})
	conn := syncMarketStream(t, venue, stream, collector)
	emittedBefore := len(collector.books())

	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63136.0", "0.50000")), "", 101, 100))
	// Wait until the stream observed the crossing, then advance the injected
	// clock past the grace — deterministic, no real sleeps.
	waitForCondition(t, "crossed observation", func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		return !stream.crossedSince.IsZero()
	})
	clock.Advance(grace)
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63136.0", "0.60000")), "", 102, 101))

	collector.waitDisconnected(t)
	venue.waitConn(t, 1)
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
	// The crossed book was never emitted.
	if got := len(collector.books()); got != emittedBefore {
		t.Fatalf("snapshots = %d, want %d (crossed books must not emit)", got, emittedBefore)
	}
}

func TestMarketStreamSendsApplicationPingsAndIgnoresPong(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, func(cfg *MarketStreamConfig) {
		cfg.PingInterval = 20 * time.Millisecond
	})
	conn := syncMarketStream(t, venue, stream, collector)

	waitForCondition(t, "application ping", func() bool {
		return venue.countInbound(pingMessage) >= 1
	})
	venue.push(t, conn, `{"type":"pong"}`)

	// Processing continues after the pong.
	venue.push(t, conn, marketBookFrame(wsTypeUpdateOrderBook, "1",
		marketLevels(marketLevel("63135.0", "0.99999")), "", 101, 100))
	collector.waitBooks(t, 2)
	for _, kind := range collector.connectionKinds() {
		if kind == "disconnected" {
			t.Fatal("pong must not disturb the stream")
		}
	}
}

func TestMarketStreamRepliesToServerPing(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	venue.push(t, conn, pingMessage)
	waitForCondition(t, "pong reply", func() bool {
		return venue.countInbound(pongMessage) >= 1
	})
}

func TestMarketStreamLifecycleFailFast(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	syncMarketStream(t, venue, stream, collector)

	if err := stream.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("second Start = %v, want already-started error", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got %v", err)
	}
	if err := stream.Start(context.Background()); !errors.Is(err, godex.ErrClosed) {
		t.Fatalf("Start after Close = %v, want ErrClosed", err)
	}
}

func TestMarketStreamFirstConnectFailureFailsStart(t *testing.T) {
	venue := newFakeMarketVenue(t)
	wsURL := venue.wsURL
	venue.server.Close()
	stream, err := NewMarketStream(MarketStreamConfig{
		Symbol:     "BTC-PERP",
		MarketID:   1,
		PriceScale: 1,
		SizeScale:  5,
		Reconnect:  testMarketReconnect(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		WSURL:      wsURL,
	})
	if err != nil {
		t.Fatalf("NewMarketStream: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	if err := stream.Start(context.Background()); err == nil {
		t.Fatal("Start against an unreachable venue must fail (no reconnect loop)")
	}
}

func TestNewMarketStreamConfigValidation(t *testing.T) {
	valid := func() MarketStreamConfig {
		return MarketStreamConfig{
			Symbol:     "BTC-PERP",
			MarketID:   1,
			PriceScale: 1,
			SizeScale:  5,
			WSURL:      "ws://venue.invalid/stream",
		}
	}
	tests := []struct {
		name    string
		mutate  func(*MarketStreamConfig)
		wantErr string
	}{
		{"missing symbol", func(c *MarketStreamConfig) { c.Symbol = "" }, "Symbol is required"},
		{"negative market id", func(c *MarketStreamConfig) { c.MarketID = -1 }, "MarketID must be non-negative"},
		{"negative price scale", func(c *MarketStreamConfig) { c.PriceScale = -1 }, "must be non-negative"},
		{"no url and no network", func(c *MarketStreamConfig) { c.WSURL = "" }, "Network must be"},
		{"partial reconnect config", func(c *MarketStreamConfig) {
			c.Reconnect = godex.ReconnectConfig{InitialDelay: time.Second}
		}, "reconnect config"},
		{"negative ping interval", func(c *MarketStreamConfig) { c.PingInterval = -time.Second }, "PingInterval must be positive"},
		{"negative crossed grace", func(c *MarketStreamConfig) { c.CrossedBookGrace = -time.Second }, "CrossedBookGrace must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid()
			tt.mutate(&cfg)
			if _, err := NewMarketStream(cfg); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
