package dydx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/book"
	"github.com/DaisukeYoda/godex/internal/ws"
)

// MarketStreamConfig parameterizes a dydx MarketStream.
type MarketStreamConfig struct {
	// Symbol is the normalized label stamped on events (e.g. "SOL-PERP").
	Symbol godex.Symbol
	// Ticker is the venue market ticker (e.g. "SOL-USD").
	Ticker string
	// PriceScale and SizeScale are the decimal scales book levels are
	// normalized to. Native precision beyond them is a configuration error
	// (fail fast), not something to round away.
	PriceScale int
	SizeScale  int
	Network    Network
	// Reconnect tunes the WS; the zero value means
	// godex.DefaultReconnectConfig().
	Reconnect godex.ReconnectConfig
	// Logger receives operational logs; nil means slog.Default().
	Logger *slog.Logger

	// Test/ops overrides. Zero values resolve from Network.
	IndexerWSURL string
}

type resolvedMarketStreamConfig struct {
	symbol       godex.Symbol
	ticker       string
	priceScale   int
	sizeScale    int
	indexerWSURL string
	reconnect    godex.ReconnectConfig
	logger       *slog.Logger
}

func (c MarketStreamConfig) resolve() (*resolvedMarketStreamConfig, error) {
	if c.Symbol == "" {
		return nil, fmt.Errorf("dydx: Symbol is required")
	}
	if c.Ticker == "" {
		return nil, fmt.Errorf("dydx: Ticker is required (e.g. \"SOL-USD\")")
	}
	if c.PriceScale < 0 || c.SizeScale < 0 {
		return nil, fmt.Errorf("dydx: PriceScale and SizeScale must be non-negative")
	}
	resolved := &resolvedMarketStreamConfig{
		symbol:       c.Symbol,
		ticker:       c.Ticker,
		priceScale:   c.PriceScale,
		sizeScale:    c.SizeScale,
		indexerWSURL: c.IndexerWSURL,
		reconnect:    c.Reconnect,
		logger:       c.Logger,
	}
	if resolved.indexerWSURL == "" {
		if c.Network != Testnet && c.Network != Mainnet {
			return nil, fmt.Errorf("dydx: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
		}
		resolved.indexerWSURL, _ = c.Network.IndexerWSURL()
	}
	if resolved.reconnect.IsZero() {
		resolved.reconnect = godex.DefaultReconnectConfig()
	}
	if err := resolved.reconnect.Validate(); err != nil {
		return nil, err
	}
	if resolved.logger == nil {
		resolved.logger = slog.Default()
	}
	return resolved, nil
}

// MarketStream streams the Indexer's v4_orderbook channel for one market and
// emits normalized full book snapshots.
//
// Sequence integrity: message_id is numbered per connection without gaps or
// duplicates, but arrival order can swap across channels. A contiguous
// watermark tracks delivery; an id gap that stays unfilled beyond
// messageReorderTolerance early arrivals, or a duplicate id, means the
// connection as a whole lost integrity — it is aborted and the automatic
// reconnect rebuilds the book from a fresh snapshot.
//
// Crossed books: the Indexer publishes crossed books in normal operation
// (event-application order jitter). Delta-driven crossings are uncrossed
// immediately by treating the later update as the freshest state (the same
// interpretation as the official client), so only a crossed snapshot reaches
// the caller side — the market is then resubscribed on the same connection.
// A within-market message_id regression (stale delta) also resubscribes.
// After maxConsecutiveResyncs the whole connection is rebuilt instead.
type MarketStream struct {
	cfg    *resolvedMarketStreamConfig
	logger *slog.Logger
	events chan godex.MarketEvent
	socket *ws.Socket

	mu      sync.Mutex
	started bool
	closed  bool
	// Connection-scoped sequence tracking.
	nextContiguousID int64
	earlyMessageIDs  map[int64]struct{}
	// Market book state.
	builder            *book.Builder
	haveBook           bool
	lastMessageID      int64
	awaitingSnapshot   bool
	consecutiveResyncs int
}

var _ godex.MarketStream = (*MarketStream)(nil)

// NewMarketStream builds a MarketStream. It does not touch the network until
// Start.
func NewMarketStream(cfg MarketStreamConfig) (*MarketStream, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	s := &MarketStream{
		cfg:    resolved,
		logger: resolved.logger,
		events: make(chan godex.MarketEvent, godex.DefaultMarketEventBuffer),
	}
	s.socket = ws.New("dydx-market", resolved.indexerWSURL, resolved.reconnect, resolved.logger, ws.Handlers{
		OnOpen:    s.handleOpen,
		OnMessage: s.handleMessage,
		OnDown:    s.handleDown,
	})
	return s, nil
}

// VenueID implements godex.MarketStream.
func (s *MarketStream) VenueID() godex.VenueID { return godex.VenueDydx }

// Events implements godex.MarketStream.
func (s *MarketStream) Events() <-chan godex.MarketEvent { return s.events }

// Start implements godex.MarketStream.
func (s *MarketStream) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return godex.ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("dydx: market stream already started")
	}
	s.started = true
	s.mu.Unlock()

	if err := s.socket.Start(ctx); err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return err
	}
	return nil
}

// Close implements godex.MarketStream.
func (s *MarketStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	started := s.started
	s.mu.Unlock()

	if started {
		// Stop delivers OnDown (the final MarketDisconnectedEvent) for an
		// open connection and waits for the read loops before returning, so
		// closing the channel afterwards cannot race an emit.
		_ = s.socket.Stop()
	}
	close(s.events)
	return nil
}

func (s *MarketStream) emit(event godex.MarketEvent) {
	s.events <- event
}

// handleOpen runs after every successful open, including reconnects: the
// sequence watermark and the book restart from scratch.
func (s *MarketStream) handleOpen() error {
	s.mu.Lock()
	s.nextContiguousID = firstMessageID
	s.earlyMessageIDs = map[int64]struct{}{}
	s.builder = nil
	s.haveBook = false
	s.awaitingSnapshot = false
	s.consecutiveResyncs = 0
	s.mu.Unlock()

	if err := s.sendSubscribe(); err != nil {
		return err
	}
	s.emit(godex.MarketConnectedEvent{VenueID: godex.VenueDydx})
	return nil
}

func (s *MarketStream) handleDown() {
	s.emit(godex.MarketDisconnectedEvent{VenueID: godex.VenueDydx})
}

// handleMessage parses and dispatches one frame. A returned error makes the
// socket abort this connection into the reconnect path (fail fast — no
// guessing at unexpected shapes).
func (s *MarketStream) handleMessage(raw []byte) error {
	message, err := decodeOrderbookWsMessage(raw)
	if err != nil {
		return err
	}
	if message.Type == wsTypePong {
		return nil
	}
	if err := s.trackSequence(message.MessageID); err != nil {
		return err
	}
	return s.dispatch(message)
}

// trackSequence advances the connection-scoped watermark. Out-of-order
// arrival is tolerated — an early message is buffered for sequence accounting
// and still dispatched (per-channel order is what matters for the book; the
// watermark only proves nothing was lost). An unfillable gap or a duplicate
// id is an integrity error that aborts the connection.
func (s *MarketStream) trackSequence(messageID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if messageID < s.nextContiguousID {
		return fmt.Errorf("dydx: duplicate message_id %d", messageID)
	}
	if messageID == s.nextContiguousID {
		s.nextContiguousID++
		for {
			if _, ok := s.earlyMessageIDs[s.nextContiguousID]; !ok {
				break
			}
			delete(s.earlyMessageIDs, s.nextContiguousID)
			s.nextContiguousID++
		}
		return nil
	}
	if _, ok := s.earlyMessageIDs[messageID]; ok {
		return fmt.Errorf("dydx: duplicate message_id %d", messageID)
	}
	s.earlyMessageIDs[messageID] = struct{}{}
	if len(s.earlyMessageIDs) > messageReorderTolerance {
		return fmt.Errorf("dydx: message_id gap (id %d never arrived)", s.nextContiguousID)
	}
	return nil
}

func (s *MarketStream) dispatch(message marketWsMessage) error {
	switch message.Type {
	case wsTypeConnected:
		return nil
	case wsTypeSubscribed:
		return s.handleSnapshot(message)
	case wsTypeUnsubscribed:
		if message.ID != s.cfg.ticker {
			return fmt.Errorf("dydx: unexpected unsubscribed for %q", message.ID)
		}
		return nil
	case wsTypeChannelData:
		return s.handleDelta(message)
	default:
		// decodeOrderbookWsMessage rejects unknown types already.
		return fmt.Errorf("dydx: unhandled ws message type %q", message.Type)
	}
}

func (s *MarketStream) handleSnapshot(message marketWsMessage) error {
	s.mu.Lock()
	if message.ID != s.cfg.ticker || !s.awaitingSnapshot {
		s.mu.Unlock()
		return fmt.Errorf("dydx: unexpected subscribed for %q", message.ID)
	}
	s.awaitingSnapshot = false
	builder := book.New(godex.VenueDydx, s.cfg.symbol, s.cfg.ticker, s.cfg.priceScale, s.cfg.sizeScale)
	s.builder = builder
	s.haveBook = true
	s.lastMessageID = message.MessageID
	s.mu.Unlock()

	bids, asks, err := message.Snapshot.toRaw()
	if err != nil {
		return err
	}
	return s.applyAndEmit(func() error { return builder.ApplySnapshot(bids, asks) })
}

func (s *MarketStream) handleDelta(message marketWsMessage) error {
	s.mu.Lock()
	if message.ID != s.cfg.ticker {
		s.mu.Unlock()
		return fmt.Errorf("dydx: channel_data for unsubscribed %q", message.ID)
	}
	if s.awaitingSnapshot {
		// An in-flight delta inside the resubscribe window; the new snapshot
		// carries the full state, so dropping it is safe.
		s.mu.Unlock()
		return nil
	}
	if !s.haveBook {
		s.mu.Unlock()
		return fmt.Errorf("dydx: channel_data before snapshot for %q", message.ID)
	}
	if message.MessageID < s.lastMessageID {
		// A within-market regression (the channel's ordering guarantee
		// broke). Applying it would corrupt the book — resubscribe instead.
		s.mu.Unlock()
		return s.resync(fmt.Sprintf("stale delta message_id %d", message.MessageID))
	}
	s.lastMessageID = message.MessageID
	builder := s.builder
	s.mu.Unlock()

	return s.applyAndEmit(func() error { return applyDydxDelta(builder, message.Delta) })
}

// applyDydxDelta interprets the [price, size] tuple delta representation,
// uncrossing against later updates (the freshest state wins).
func applyDydxDelta(builder *book.Builder, delta *wsBookDelta) error {
	for side, tuples := range map[book.Side][][]string{book.Bids: delta.Bids, book.Asks: delta.Asks} {
		for _, tuple := range tuples {
			level, err := builder.ApplyLevel(side, tuple[0], tuple[1])
			if err != nil {
				return err
			}
			if level != nil {
				builder.RemoveCrossedLevels(side, level.Price)
			}
		}
	}
	return nil
}

// applyAndEmit applies a book update and emits the snapshot. Delta-driven
// crossings are uncrossed by the builder, so a book still crossed here came
// from a snapshot (which has no update ordering to uncross by) — it is
// discarded and the market resubscribed (a crossed book is never emitted).
func (s *MarketStream) applyAndEmit(apply func() error) error {
	if err := apply(); err != nil {
		return err
	}
	s.mu.Lock()
	builder := s.builder
	s.mu.Unlock()
	if builder.IsCrossed() {
		return s.resync("crossed snapshot")
	}
	snapshot, err := builder.Snapshot(time.Now())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.consecutiveResyncs = 0
	s.mu.Unlock()
	s.emit(godex.BookSnapshotEvent{Book: snapshot})
	return nil
}

// resync discards the market's book and resubscribes on the same connection.
// Past maxConsecutiveResyncs the whole connection is rebuilt instead (with
// reconnect backoff), per the Indexer's rate-limit guidance.
func (s *MarketStream) resync(reason string) error {
	s.mu.Lock()
	s.consecutiveResyncs++
	consecutive := s.consecutiveResyncs
	s.builder = nil
	s.haveBook = false
	s.mu.Unlock()

	if consecutive > maxConsecutiveResyncs {
		return fmt.Errorf("dydx: %q still inconsistent after %d resyncs", s.cfg.ticker, consecutive-1)
	}
	s.logger.Warn("dydx market stream resubscribing", "ticker", s.cfg.ticker, "reason", reason)
	unsubscribe, err := json.Marshal(map[string]string{
		"type": "unsubscribe", "channel": orderbookChannel, "id": s.cfg.ticker,
	})
	if err != nil {
		return err
	}
	if err := s.socket.Send(string(unsubscribe)); err != nil {
		return err
	}
	return s.sendSubscribe()
}

func (s *MarketStream) sendSubscribe() error {
	s.mu.Lock()
	s.awaitingSnapshot = true
	s.mu.Unlock()
	subscribe, err := json.Marshal(map[string]string{
		"type": "subscribe", "channel": orderbookChannel, "id": s.cfg.ticker,
	})
	if err != nil {
		return err
	}
	return s.socket.Send(string(subscribe))
}
