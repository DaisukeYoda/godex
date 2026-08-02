package lighter

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/internal/book"
	"github.com/DaisukeYoda/godex/internal/ws"
)

// MarketStreamConfig parameterizes a lighter MarketStream.
type MarketStreamConfig struct {
	// Symbol is the normalized label stamped on events (e.g. "SOL-PERP").
	Symbol godex.Symbol
	// MarketID is the venue's numeric market identifier.
	MarketID int64
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

	// Test/ops overrides. Zero values resolve from Network and the package
	// constants.
	WSURL            string
	PingInterval     time.Duration
	CrossedBookGrace time.Duration
	Now              func() time.Time
}

type resolvedMarketStreamConfig struct {
	symbol           godex.Symbol
	marketID         int64
	marketIDLabel    string
	priceScale       int
	sizeScale        int
	wsURL            string
	reconnect        godex.ReconnectConfig
	logger           *slog.Logger
	pingInterval     time.Duration
	crossedBookGrace time.Duration
	now              func() time.Time
}

func (c MarketStreamConfig) resolve() (*resolvedMarketStreamConfig, error) {
	if c.Symbol == "" {
		return nil, fmt.Errorf("lighter: Symbol is required")
	}
	if c.MarketID < 0 {
		return nil, fmt.Errorf("lighter: MarketID must be non-negative")
	}
	if c.PriceScale < 0 || c.SizeScale < 0 {
		return nil, fmt.Errorf("lighter: PriceScale and SizeScale must be non-negative")
	}
	resolved := &resolvedMarketStreamConfig{
		symbol:           c.Symbol,
		marketID:         c.MarketID,
		marketIDLabel:    strconv.FormatInt(c.MarketID, 10),
		priceScale:       c.PriceScale,
		sizeScale:        c.SizeScale,
		wsURL:            c.WSURL,
		reconnect:        c.Reconnect,
		logger:           c.Logger,
		pingInterval:     c.PingInterval,
		crossedBookGrace: c.CrossedBookGrace,
		now:              c.Now,
	}
	if resolved.wsURL == "" {
		if c.Network != Testnet && c.Network != Mainnet {
			return nil, fmt.Errorf("lighter: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
		}
		resolved.wsURL, _ = c.Network.WSURL()
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
	if resolved.pingInterval == 0 {
		resolved.pingInterval = pingInterval
	}
	if resolved.pingInterval <= 0 {
		return nil, fmt.Errorf("lighter: PingInterval must be positive")
	}
	if resolved.crossedBookGrace == 0 {
		resolved.crossedBookGrace = defaultCrossedBookGrace
	}
	if resolved.crossedBookGrace <= 0 {
		return nil, fmt.Errorf("lighter: CrossedBookGrace must be positive")
	}
	if resolved.now == nil {
		resolved.now = time.Now
	}
	return resolved, nil
}

// MarketStream streams the public order_book channel for one market and emits
// normalized full book snapshots.
//
// Sequence integrity: an update's begin_nonce must equal the previous frame's
// nonce (official WS reference). A mismatch means the stream lost continuity —
// the connection is aborted and the automatic reconnect rebuilds the book from
// a fresh snapshot. There is no official per-market resubscribe, so unlike the
// dydx stream every integrity failure escalates straight to the connection.
//
// Crossed books: nonce continuity should make them impossible. If one appears
// anyway, emits are suppressed while waiting for natural resolution (a broken
// book is never shown), and past CrossedBookGrace the connection is rebuilt.
//
// The server drops connections after 2 minutes of silence; an application
// {"type":"ping"} is sent every PingInterval while the stream lives.
type MarketStream struct {
	cfg    *resolvedMarketStreamConfig
	logger *slog.Logger
	events chan godex.MarketEvent
	socket *ws.Socket

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	pingWG          sync.WaitGroup

	mu      sync.Mutex
	started bool
	closed  bool
	// Market book state.
	builder          *book.Builder
	haveBook         bool
	lastNonce        int64
	awaitingSnapshot bool
	// crossedSince is the first observation time of the current crossed
	// state; zero when the book is not crossed.
	crossedSince time.Time
}

var _ godex.MarketStream = (*MarketStream)(nil)

// NewMarketStream builds a MarketStream. It does not touch the network until
// Start.
func NewMarketStream(cfg MarketStreamConfig) (*MarketStream, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	s := &MarketStream{
		cfg:             resolved,
		logger:          resolved.logger,
		events:          make(chan godex.MarketEvent, godex.DefaultMarketEventBuffer),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
	s.socket = ws.New("lighter-market", resolved.wsURL, resolved.reconnect, resolved.logger, ws.Handlers{
		OnOpen:    s.handleOpen,
		OnMessage: s.handleMessage,
		OnDown:    s.handleDown,
	})
	return s, nil
}

// VenueID implements godex.MarketStream.
func (s *MarketStream) VenueID() godex.VenueID { return godex.VenueLighter }

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
		return fmt.Errorf("lighter: market stream already started")
	}
	s.started = true
	s.mu.Unlock()

	if err := s.socket.Start(ctx); err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return err
	}
	s.pingWG.Add(1)
	go s.pingLoop()
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

	s.lifecycleCancel()
	s.pingWG.Wait()
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

func (s *MarketStream) pingLoop() {
	defer s.pingWG.Done()
	ticker := time.NewTicker(s.cfg.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if s.socket.IsOpen() {
				// A lost race with a concurrent disconnect is harmless.
				_ = s.socket.Send(pingMessage)
			}
		}
	}
}

// handleOpen runs after every successful open, including reconnects: nonce
// tracking and the book restart from scratch.
func (s *MarketStream) handleOpen() error {
	s.mu.Lock()
	s.builder = nil
	s.haveBook = false
	s.awaitingSnapshot = true
	s.crossedSince = time.Time{}
	s.mu.Unlock()

	subscribe := fmt.Sprintf(`{"type":"subscribe","channel":"%s/%s"}`, orderbookChannel, s.cfg.marketIDLabel)
	if err := s.socket.Send(subscribe); err != nil {
		return err
	}
	s.emit(godex.MarketConnectedEvent{VenueID: godex.VenueLighter})
	return nil
}

func (s *MarketStream) handleDown() {
	s.emit(godex.MarketDisconnectedEvent{VenueID: godex.VenueLighter})
}

// handleMessage parses and dispatches one frame. A returned error makes the
// socket abort this connection into the reconnect path (fail fast — no
// guessing at unexpected shapes).
func (s *MarketStream) handleMessage(raw []byte) error {
	message, err := decodeMarketWsMessage(raw)
	if err != nil {
		return err
	}
	switch message.Type {
	case wsTypePing:
		// Reply to server-initiated pings (as the reference ws client does).
		return s.socket.Send(pongMessage)
	case wsTypeConnected, wsTypePong:
		return nil
	case wsTypeSubscribedOrderBook:
		return s.handleSnapshot(message)
	case wsTypeUpdateOrderBook:
		return s.handleUpdate(message)
	default:
		// decodeMarketWsMessage rejects unknown types already.
		return fmt.Errorf("lighter: unhandled ws message type %q", message.Type)
	}
}

func (s *MarketStream) handleSnapshot(message marketWsMessage) error {
	s.mu.Lock()
	if message.MarketID != s.cfg.marketIDLabel || !s.awaitingSnapshot {
		s.mu.Unlock()
		return fmt.Errorf("lighter: unexpected snapshot for market %s", message.MarketID)
	}
	s.awaitingSnapshot = false
	builder := book.New(godex.VenueLighter, s.cfg.symbol, s.cfg.marketIDLabel, s.cfg.priceScale, s.cfg.sizeScale)
	s.builder = builder
	s.haveBook = true
	s.lastNonce = *message.Book.Nonce
	s.mu.Unlock()

	bids, asks, err := message.Book.toRaw("snapshot")
	if err != nil {
		return err
	}
	return s.applyAndEmit(func() error { return builder.ApplySnapshot(bids, asks) })
}

func (s *MarketStream) handleUpdate(message marketWsMessage) error {
	s.mu.Lock()
	if message.MarketID != s.cfg.marketIDLabel || !s.haveBook {
		s.mu.Unlock()
		return fmt.Errorf("lighter: update for unsubscribed market %s", message.MarketID)
	}
	if *message.Book.BeginNonce != s.lastNonce {
		lastNonce := s.lastNonce
		s.mu.Unlock()
		return fmt.Errorf("lighter: market %s nonce gap (begin_nonce %d != last nonce %d)",
			message.MarketID, *message.Book.BeginNonce, lastNonce)
	}
	s.lastNonce = *message.Book.Nonce
	builder := s.builder
	s.mu.Unlock()

	bids, asks, err := message.Book.toRaw("update")
	if err != nil {
		return err
	}
	return s.applyAndEmit(func() error {
		for _, level := range bids {
			if _, err := builder.ApplyLevel(book.Bids, level.Price, level.Size); err != nil {
				return err
			}
		}
		for _, level := range asks {
			if _, err := builder.ApplyLevel(book.Asks, level.Price, level.Size); err != nil {
				return err
			}
		}
		return nil
	})
}

// applyAndEmit applies a book update and emits the snapshot. A crossed book
// suppresses emits while waiting for natural resolution; past
// CrossedBookGrace the connection is rebuilt. While suppressed, consumers'
// staleness thresholds retire the last emitted book — an old book is never
// presented as current.
func (s *MarketStream) applyAndEmit(apply func() error) error {
	if err := apply(); err != nil {
		return err
	}
	s.mu.Lock()
	builder := s.builder
	s.mu.Unlock()
	if builder.IsCrossed() {
		now := s.cfg.now()
		s.mu.Lock()
		since := s.crossedSince
		if since.IsZero() {
			s.crossedSince = now
		}
		s.mu.Unlock()
		if since.IsZero() {
			s.logger.Warn("lighter market stream book crossed — suppressing emits",
				"marketId", s.cfg.marketIDLabel, "grace", s.cfg.crossedBookGrace)
			return nil
		}
		if elapsed := now.Sub(since); elapsed >= s.cfg.crossedBookGrace {
			return fmt.Errorf("lighter: market %s book crossed for %s", s.cfg.marketIDLabel, elapsed)
		}
		return nil
	}
	s.mu.Lock()
	s.crossedSince = time.Time{}
	s.mu.Unlock()
	snapshot, err := builder.Snapshot(s.cfg.now())
	if err != nil {
		return err
	}
	s.emit(godex.BookSnapshotEvent{Book: snapshot})
	return nil
}
