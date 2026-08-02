package dydx

import (
	"context"
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
// dydx-connector.test.ts) onto the Go MarketStream: snapshot+delta
// reassembly with uncrossing, message_id watermark tracking, per-market
// resync, and the escalation to a connection rebuild.

const marketTestTimeout = 2 * time.Second

// fakeMarketVenue is an httptest-backed v4_orderbook endpoint the tests push
// frames into.
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
			venue.mu.Lock()
			venue.inbound = append(venue.inbound, string(data))
			venue.mu.Unlock()
		}
	})
	venue.server = httptest.NewServer(mux)
	venue.wsURL = "ws" + strings.TrimPrefix(venue.server.URL, "http") + "/v4/ws"
	t.Cleanup(venue.server.Close)
	return venue
}

func (v *fakeMarketVenue) connCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

func (v *fakeMarketVenue) waitConn(t *testing.T, index int) *websocket.Conn {
	t.Helper()
	waitForMarketCondition(t, fmt.Sprintf("connection %d", index), func() bool {
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
	waitForMarketCondition(t, fmt.Sprintf("%d book snapshots", count), func() bool {
		return len(c.books()) >= count
	})
	return c.books()
}

// waitConnectionKinds waits for the exact connected/disconnected sequence.
// Tests that abort a connection wait for the trailing "connected" of the
// automatic reconnect so teardown does not overlap an in-flight open.
func (c *marketCollector) waitConnectionKinds(t *testing.T, want ...string) {
	t.Helper()
	waitForMarketCondition(t, fmt.Sprintf("connection sequence %v", want), func() bool {
		return fmt.Sprint(c.connectionKinds()) == fmt.Sprint(want)
	})
}

func waitForMarketCondition(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(marketTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func testMarketReconnect() godex.ReconnectConfig {
	return godex.ReconnectConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2,
		IdleTimeout:  time.Second,
	}
}

// newTestMarketStream builds a SOL-USD stream against the fake venue; mutate
// adjusts the config before construction.
func newTestMarketStream(t *testing.T, venue *fakeMarketVenue, mutate func(*MarketStreamConfig)) (*MarketStream, *marketCollector) {
	t.Helper()
	cfg := MarketStreamConfig{
		Symbol:       "SOL-PERP",
		Ticker:       "SOL-USD",
		PriceScale:   4,
		SizeScale:    3,
		Reconnect:    testMarketReconnect(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		IndexerWSURL: venue.wsURL,
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

const (
	subscribeSOLMessage   = `{"channel":"v4_orderbook","id":"SOL-USD","type":"subscribe"}`
	unsubscribeSOLMessage = `{"channel":"v4_orderbook","id":"SOL-USD","type":"unsubscribe"}`
)

func connectedFrame(messageID int64) string {
	return fmt.Sprintf(`{"type":"connected","message_id":%d}`, messageID)
}

// snapshotFrame formats a subscribed frame; levels are {price,size} objects.
func snapshotFrame(messageID int64, bids, asks string) string {
	return fmt.Sprintf(`{"type":"subscribed","message_id":%d,"channel":"v4_orderbook","id":"SOL-USD","contents":{"bids":[%s],"asks":[%s]}}`,
		messageID, bids, asks)
}

// deltaFrame formats a channel_data frame; levels are [price, size] tuples.
func deltaFrame(messageID int64, bids, asks string) string {
	return fmt.Sprintf(`{"type":"channel_data","message_id":%d,"channel":"v4_orderbook","id":"SOL-USD","contents":{"bids":[%s],"asks":[%s]}}`,
		messageID, bids, asks)
}

func objLevel(price, size string) string {
	return fmt.Sprintf(`{"price":"%s","size":"%s"}`, price, size)
}

func tupleLevel(price, size string) string {
	return fmt.Sprintf(`["%s","%s"]`, price, size)
}

// solSnapshotFrame mirrors the reference fixture: bids intentionally unsorted
// to verify output sorting.
func solSnapshotFrame(messageID int64) string {
	return snapshotFrame(messageID,
		strings.Join([]string{
			objLevel("128.4100", "10.000"),
			objLevel("128.4300", "5.000"),
			objLevel("128.4000", "20.000"),
		}, ","),
		strings.Join([]string{
			objLevel("128.4400", "7.500"),
			objLevel("128.5000", "40.000"),
		}, ","))
}

// syncMarketStream starts the stream and drives it to connected + first
// snapshot, asserting the subscribe wire format on the way. The connected
// frame consumes message_id 0.
func syncMarketStream(t *testing.T, venue *fakeMarketVenue, stream *MarketStream, collector *marketCollector) *websocket.Conn {
	t.Helper()
	if err := stream.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn := venue.waitConn(t, 0)
	waitForMarketCondition(t, "subscribe message", func() bool {
		return venue.countInbound(subscribeSOLMessage) >= 1
	})
	venue.push(t, conn, connectedFrame(0))
	venue.push(t, conn, solSnapshotFrame(1))
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

func askStrings(book godex.OrderBook) [][2]string {
	levels := make([][2]string, 0, len(book.Asks))
	for _, level := range book.Asks {
		levels = append(levels, [2]string{level.Price.String(), level.Size.String()})
	}
	return levels
}

// TS: "snapshot+deltaから正規化済みフル板をemitする"
func TestMarketStreamNormalizesSnapshotAndAppliesDeltas(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	first := collector.books()[0]
	if first.VenueID != godex.VenueDydx || first.Symbol != "SOL-PERP" {
		t.Fatalf("unexpected identity: %s %s", first.VenueID, first.Symbol)
	}
	wantBids := [][2]string{{"128.4300", "5.000"}, {"128.4100", "10.000"}, {"128.4000", "20.000"}}
	if got := bidStrings(first); fmt.Sprint(got) != fmt.Sprint(wantBids) {
		t.Fatalf("bids = %v, want %v", got, wantBids)
	}
	wantAsks := [][2]string{{"128.4400", "7.500"}, {"128.5000", "40.000"}}
	if got := askStrings(first); fmt.Sprint(got) != fmt.Sprint(wantAsks) {
		t.Fatalf("asks = %v, want %v", got, wantAsks)
	}

	// Delta: replace one level, remove one (size "0").
	venue.push(t, conn, deltaFrame(2,
		strings.Join([]string{tupleLevel("128.4300", "6.000"), tupleLevel("128.4000", "0")}, ","),
		""))
	second := collector.waitBooks(t, 2)[1]
	wantBids = [][2]string{{"128.4300", "6.000"}, {"128.4100", "10.000"}}
	if got := bidStrings(second); fmt.Sprint(got) != fmt.Sprint(wantBids) {
		t.Fatalf("bids after delta = %v, want %v", got, wantBids)
	}
}

// TS: "delta起因の交差は後着更新を優先して即時uncrossする"
func TestMarketStreamUncrossesDeltaDrivenCrossings(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	// A bid that crosses the best ask: the stale ask levels at or below it are
	// removed rather than emitting a crossed book.
	venue.push(t, conn, deltaFrame(2, tupleLevel("128.4500", "3.000"), ""))
	second := collector.waitBooks(t, 2)[1]
	if got := second.Bids[0].Price.String(); got != "128.4500" {
		t.Fatalf("best bid = %s, want 128.4500", got)
	}
	wantAsks := [][2]string{{"128.5000", "40.000"}}
	if got := askStrings(second); fmt.Sprint(got) != fmt.Sprint(wantAsks) {
		t.Fatalf("asks after uncross = %v, want %v", got, wantAsks)
	}
}

// TS: "message_idの乱序を許容する(チャンネル間の入れ替わり)" — in a
// single-market stream the realistic reordering is between the market channel
// and control acks: here the resubscribe snapshot overtakes the unsubscribed
// ack. The watermark buffers the early id instead of reading it as a gap.
func TestMarketStreamToleratesReorderedMessageIDs(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)

	// A crossed first snapshot (id 1) forces a resync: unsubscribe +
	// resubscribe on the same connection.
	if err := stream.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn := venue.waitConn(t, 0)
	venue.push(t, conn, connectedFrame(0))
	venue.push(t, conn, snapshotFrame(1, objLevel("128.5000", "1.000"), objLevel("128.4000", "1.000")))
	waitForMarketCondition(t, "resubscribe", func() bool {
		return venue.countInbound(unsubscribeSOLMessage) >= 1 && venue.countInbound(subscribeSOLMessage) >= 2
	})

	// The fresh snapshot (id 3) overtakes the unsubscribed ack (id 2): the
	// early id is buffered, not read as a gap, and both are dispatched.
	venue.push(t, conn, snapshotFrame(3, objLevel("128.4100", "10.000"), objLevel("128.4400", "7.500")))
	venue.push(t, conn, `{"type":"unsubscribed","message_id":2,"channel":"v4_orderbook","id":"SOL-USD"}`)
	books := collector.waitBooks(t, 1)
	if got := books[0].Bids[0].Size.String(); got != "10.000" {
		t.Fatalf("best bid size after reordered recovery = %s, want 10.000", got)
	}
	// A follow-up delta proves the connection survived both frames.
	venue.push(t, conn, deltaFrame(4, tupleLevel("128.4100", "11.000"), ""))
	collector.waitBooks(t, 2)
	if venue.connCount() != 1 {
		t.Fatalf("reordering must not rebuild the connection (%d conns)", venue.connCount())
	}
}

// TS: "既知idの重複で強制切断→自動再接続で全銘柄を作り直す"
func TestMarketStreamDuplicateMessageIDRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	venue.push(t, conn, deltaFrame(1, tupleLevel("128.4300", "6.000"), ""))
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")

	// The rebuilt connection resubscribes and recovers a fresh book.
	conn2 := venue.waitConn(t, 1)
	waitForMarketCondition(t, "resubscribe", func() bool {
		return venue.countInbound(subscribeSOLMessage) >= 2
	})
	venue.push(t, conn2, connectedFrame(0))
	venue.push(t, conn2, solSnapshotFrame(1))
	collector.waitBooks(t, 2)
}

// TS: "REORDER_TOLERANCEを超えて埋まらない欠番(真のギャップ)で強制切断"
func TestMarketStreamUnfilledGapRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	// id 2 never arrives; flood ids 3.. until the early buffer overflows.
	for id := int64(3); id <= int64(3+messageReorderTolerance); id++ {
		venue.push(t, conn, deltaFrame(id, tupleLevel("128.4300", "6.000"), ""))
	}
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
}

// TS: "同一銘柄内のmessage_id逆行(stale delta)は同一接続上で再購読する"
func TestMarketStreamStaleDeltaResubscribesOnSameConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	// Delta id 3 overtakes id 2: the early id is buffered by the watermark and
	// dispatched (lastMessageID = 3). When id 2 then arrives it passes the
	// watermark (it fills the gap) but regresses within the market — applying
	// it would corrupt the book, so the market resubscribes.
	venue.push(t, conn, deltaFrame(3, tupleLevel("128.4300", "7.000"), ""))
	collector.waitBooks(t, 2)
	venue.push(t, conn, deltaFrame(2, tupleLevel("128.4300", "8.000"), ""))

	waitForMarketCondition(t, "unsubscribe+resubscribe", func() bool {
		return venue.countInbound(unsubscribeSOLMessage) >= 1 && venue.countInbound(subscribeSOLMessage) >= 2
	})
	if venue.connCount() != 1 {
		t.Fatalf("stale delta must resync on the same connection (%d conns)", venue.connCount())
	}

	// In-flight deltas during the resubscribe window are dropped, and the new
	// snapshot restores the book.
	booksBefore := len(collector.books())
	venue.push(t, conn, deltaFrame(4, tupleLevel("128.4300", "9.000"), ""))
	venue.push(t, conn, snapshotFrame(5, objLevel("128.4100", "10.000"), objLevel("128.4400", "7.500")))
	books := collector.waitBooks(t, booksBefore+1)
	last := books[len(books)-1]
	if got := last.Bids[0].Size.String(); got != "10.000" {
		t.Fatalf("post-resync best bid size = %s, want 10.000 (in-flight delta must be dropped)", got)
	}
}

// duplicate frame id: stale deltas beyond the watermark are indistinguishable
// from duplicates, so this arrives as its own scenario — a message_id below
// the contiguous watermark tears the connection down (see
// TestMarketStreamDuplicateMessageIDRebuildsConnection).

// TS: "交差したsnapshotは破棄して同一接続上で再購読する"
func TestMarketStreamCrossedSnapshotResubscribes(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)

	if err := stream.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn := venue.waitConn(t, 0)
	venue.push(t, conn, connectedFrame(0))
	// A crossed snapshot: best bid above best ask. Snapshots cannot be
	// uncrossed (no update ordering), so the market resubscribes.
	venue.push(t, conn, snapshotFrame(1, objLevel("128.5000", "1.000"), objLevel("128.4000", "1.000")))
	waitForMarketCondition(t, "resubscribe after crossed snapshot", func() bool {
		return venue.countInbound(unsubscribeSOLMessage) >= 1 && venue.countInbound(subscribeSOLMessage) >= 2
	})
	if books := collector.books(); len(books) != 0 {
		t.Fatalf("a crossed snapshot must not be emitted (%d books)", len(books))
	}

	// A clean snapshot on the resubscribe recovers.
	venue.push(t, conn, snapshotFrame(2, objLevel("128.4100", "10.000"), objLevel("128.4400", "7.500")))
	collector.waitBooks(t, 1)
}

// TS: "連続再購読の上限超過は接続全体の作り直しへエスカレーションする"
func TestMarketStreamResyncLimitEscalatesToConnectionRebuild(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)

	if err := stream.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	conn := venue.waitConn(t, 0)
	venue.push(t, conn, connectedFrame(0))
	// Each crossed snapshot triggers one resync; past maxConsecutiveResyncs
	// the connection is torn down instead.
	for id := int64(1); id <= int64(maxConsecutiveResyncs)+1; id++ {
		venue.push(t, conn, snapshotFrame(id, objLevel("128.5000", "1.000"), objLevel("128.4000", "1.000")))
	}
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
	if got := venue.countInbound(unsubscribeSOLMessage); got != maxConsecutiveResyncs {
		t.Fatalf("expected exactly %d resubscribe attempts before escalation, got %d", maxConsecutiveResyncs, got)
	}
}

// TS: "購読外銘柄のchannel_dataは契約違反として切断する"
func TestMarketStreamForeignChannelDataRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	venue.push(t, conn, `{"type":"channel_data","message_id":2,"channel":"v4_orderbook","id":"ETH-USD","contents":{}}`)
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
}

// The venue error frame aborts the connection (FailFast, no guessing).
func TestMarketStreamVenueErrorRebuildsConnection(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	conn := syncMarketStream(t, venue, stream, collector)

	venue.push(t, conn, `{"type":"error","message":"Too many subscribe attempts"}`)
	collector.waitConnectionKinds(t, "connected", "disconnected", "connected")
}

func TestMarketStreamFirstConnectFailureFailsStart(t *testing.T) {
	stream, err := NewMarketStream(MarketStreamConfig{
		Symbol:       "SOL-PERP",
		Ticker:       "SOL-USD",
		PriceScale:   4,
		SizeScale:    3,
		Reconnect:    testMarketReconnect(),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		IndexerWSURL: "ws://127.0.0.1:1/v4/ws",
	})
	if err != nil {
		t.Fatalf("NewMarketStream: %v", err)
	}
	if err := stream.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail fast on the first connect")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close after failed Start: %v", err)
	}
}

func TestMarketStreamLifecycle(t *testing.T) {
	venue := newFakeMarketVenue(t)
	stream, collector := newTestMarketStream(t, venue, nil)
	syncMarketStream(t, venue, stream, collector)

	if err := stream.Start(context.Background()); err == nil {
		t.Fatal("expected a second Start to fail")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-collector.done // channel closes only after Close
	kinds := collector.connectionKinds()
	if kinds[len(kinds)-1] != "disconnected" {
		t.Fatalf("expected a final disconnected event, got %v", kinds)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close must be idempotent: %v", err)
	}
	if err := stream.Start(context.Background()); err != godex.ErrClosed {
		t.Fatalf("Start after Close = %v, want ErrClosed", err)
	}
}

func TestNewMarketStreamConfigValidation(t *testing.T) {
	base := func() MarketStreamConfig {
		return MarketStreamConfig{
			Symbol:       "SOL-PERP",
			Ticker:       "SOL-USD",
			PriceScale:   4,
			SizeScale:    3,
			IndexerWSURL: "ws://example.invalid/v4/ws",
		}
	}
	cases := []struct {
		name   string
		mutate func(*MarketStreamConfig)
	}{
		{"missing symbol", func(c *MarketStreamConfig) { c.Symbol = "" }},
		{"missing ticker", func(c *MarketStreamConfig) { c.Ticker = "" }},
		{"negative price scale", func(c *MarketStreamConfig) { c.PriceScale = -1 }},
		{"negative size scale", func(c *MarketStreamConfig) { c.SizeScale = -1 }},
		{"no url and no network", func(c *MarketStreamConfig) { c.IndexerWSURL = "" }},
		{"partial reconnect config", func(c *MarketStreamConfig) {
			c.Reconnect = godex.ReconnectConfig{InitialDelay: time.Second}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			if _, err := NewMarketStream(cfg); err == nil {
				t.Fatal("expected a config error")
			}
		})
	}
}
