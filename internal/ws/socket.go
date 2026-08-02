// Package ws implements the shared WebSocket connection lifecycle used by
// venue adapters: exponential-backoff reconnect and half-open detection.
// Adapters own message handling and subscription management.
//
// Behavior contract:
//   - Between Start and Stop, unexpected drops reconnect automatically with
//     exponential backoff (reset on every successful open).
//   - A first-connect failure fails Start and never enters the reconnect
//     loop (fail fast).
//   - A connection with no inbound traffic (messages, pings, pongs) for
//     IdleTimeout is treated as half-open — sleep/wake or a network partition
//     where no close frame arrives — and is force-closed into the reconnect
//     path. The watchdog also sends protocol pings each tick so quiet
//     channels (account streams) prove liveness via mandatory pongs.
package ws

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/gorilla/websocket"
)

// controlWriteTimeout bounds writes of ping/pong control frames so a stalled
// peer cannot block the watchdog.
const controlWriteTimeout = 5 * time.Second

// Handlers are the adapter-side hooks. All hooks are required.
type Handlers struct {
	// OnOpen runs after every successful open, including reconnects; use it
	// to (re)subscribe. An error is treated as loss of this connection: the
	// socket aborts into the reconnect path (the process stays up).
	OnOpen func() error
	// OnMessage receives each inbound message. An error (protocol violation,
	// venue error notice) marks the connection unreliable: all subscriptions
	// are discarded and the socket reconnects.
	OnMessage func(data []byte) error
	// OnDown runs when an open connection drops. It is not called for failed
	// reconnect attempts.
	OnDown func()
}

// Socket is a reconnecting WebSocket connection.
type Socket struct {
	label    string
	url      string
	cfg      godex.ReconnectConfig
	handlers Handlers
	logger   *slog.Logger

	// lastInboundNano is the unix-nano timestamp of the latest inbound
	// evidence (message, ping, or pong frame).
	lastInboundNano atomic.Int64

	writeMu sync.Mutex // gorilla allows a single concurrent message writer

	mu             sync.Mutex
	conn           *websocket.Conn
	open           bool
	running        bool
	delay          time.Duration
	reconnectTimer *time.Timer
	lifecycle      context.Context
	cancel         context.CancelFunc
	// wg tracks read loops and watchdogs of all connections, plus in-flight
	// reconnect dials. The dials matter: OnOpen runs on the reconnect
	// goroutine, and Stop must not return while an OnOpen (which typically
	// emits into an adapter's event channel) may still be running — the
	// adapter closes that channel right after Stop.
	wg sync.WaitGroup
}

// New builds a Socket. cfg must be validated by the caller; logger is
// required.
func New(label, url string, cfg godex.ReconnectConfig, logger *slog.Logger, handlers Handlers) *Socket {
	if handlers.OnOpen == nil || handlers.OnMessage == nil || handlers.OnDown == nil {
		panic("ws: all handlers are required")
	}
	if logger == nil {
		panic("ws: logger is required")
	}
	return &Socket{
		label:    label,
		url:      url,
		cfg:      cfg,
		handlers: handlers,
		logger:   logger,
	}
}

// IsOpen reports whether the connection is currently open.
func (s *Socket) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open
}

// Start dials the first connection. A failure is returned to the caller and
// the reconnect loop is not entered (fail fast).
func (s *Socket) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("ws: %s: already connected", s.label)
	}
	s.running = true
	s.delay = s.cfg.InitialDelay
	s.lifecycle, s.cancel = context.WithCancel(context.Background())
	s.mu.Unlock()

	if err := s.dial(ctx); err != nil {
		s.mu.Lock()
		s.running = false
		s.stopReconnectTimerLocked()
		s.cancel()
		s.mu.Unlock()
		return fmt.Errorf("ws: %s: connect: %w", s.label, err)
	}
	return nil
}

// Stop closes the connection and halts reconnecting. OnDown is delivered for
// an open connection before Stop returns.
func (s *Socket) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return fmt.Errorf("ws: %s: not connected", s.label)
	}
	s.running = false
	s.stopReconnectTimerLocked()
	s.cancel()
	conn := s.conn
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	s.wg.Wait()
	return nil
}

// Send writes a text message to the open connection.
func (s *Socket) Send(text string) error {
	s.mu.Lock()
	conn, open := s.conn, s.open
	s.mu.Unlock()
	if !open || conn == nil {
		return fmt.Errorf("ws: %s: cannot send without an open socket", s.label)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, []byte(text))
}

// Abort force-closes the current connection, putting it on the automatic
// reconnect path. Used when per-connection sequence integrity breaks and all
// subscriptions must be rebuilt.
func (s *Socket) Abort() {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (s *Socket) touchInbound() {
	s.lastInboundNano.Store(time.Now().UnixNano())
}

func (s *Socket) dial(ctx context.Context) error {
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, s.url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return err
	}

	s.touchInbound()
	conn.SetPingHandler(func(appData string) error {
		// Overriding the handler disables gorilla's automatic pong, so send
		// it ourselves; ping frames also count as inbound liveness evidence.
		s.touchInbound()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(controlWriteTimeout))
	})
	conn.SetPongHandler(func(string) error {
		s.touchInbound()
		return nil
	})

	watchdogStop := make(chan struct{})
	s.mu.Lock()
	if !s.running {
		// Stop won the race against an in-flight dial: drop the connection
		// instead of resurrecting a stopped socket.
		s.mu.Unlock()
		_ = conn.Close()
		return context.Canceled
	}
	s.conn = conn
	s.open = true
	s.delay = s.cfg.InitialDelay // reset backoff on success
	s.mu.Unlock()

	s.wg.Add(2)
	go s.watchdog(conn, watchdogStop)
	go s.readLoop(conn, watchdogStop)

	if err := s.handlers.OnOpen(); err != nil {
		// Subscribing right after open failed: treat as loss of this
		// connection (the process stays up, the venue stays isolated).
		s.logger.Error("ws open handling failed — reconnecting", "label", s.label, "error", err)
		_ = conn.Close()
	}
	return nil
}

func (s *Socket) readLoop(conn *websocket.Conn, watchdogStop chan struct{}) {
	defer s.wg.Done()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		s.touchInbound()
		if err := s.handlers.OnMessage(data); err != nil {
			s.logger.Error("ws message handling failed — reconnecting", "label", s.label, "error", err)
			break
		}
	}
	_ = conn.Close()
	close(watchdogStop)
	s.handleClose(conn)
}

// watchdog detects half-open connections: it pings each tick and force-closes
// the connection when nothing inbound arrives for IdleTimeout.
func (s *Socket) watchdog(conn *websocket.Conn, stop chan struct{}) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.IdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			idle := time.Since(time.Unix(0, s.lastInboundNano.Load()))
			if idle > s.cfg.IdleTimeout {
				s.logger.Error("ws no inbound traffic (half-open) — reconnecting",
					"label", s.label, "idle", idle, "idleTimeout", s.cfg.IdleTimeout)
				_ = conn.Close()
				return
			}
			// Ping every tick so even quiet channels prove liveness: pong
			// responses are mandatory (RFC 6455) and count as inbound.
			_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(controlWriteTimeout))
		}
	}
}

func (s *Socket) handleClose(conn *websocket.Conn) {
	s.mu.Lock()
	if s.conn != conn {
		// A stale close from an already-replaced connection.
		s.mu.Unlock()
		return
	}
	s.conn = nil
	wasOpen := s.open
	s.open = false
	running := s.running
	s.mu.Unlock()

	if wasOpen {
		s.handlers.OnDown()
	}
	if running {
		s.scheduleReconnect()
	}
}

func (s *Socket) scheduleReconnect() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	delay := s.delay
	s.delay = min(time.Duration(float64(s.delay)*s.cfg.Multiplier), s.cfg.MaxDelay)
	lifecycle := s.lifecycle
	s.reconnectTimer = time.AfterFunc(delay, func() {
		// Register with wg under mu while running is still true, so Stop
		// (which flips running under the same mutex before waiting) either
		// sees this dial and waits for it — OnOpen included — or this
		// callback sees the stop and never dials. wg.Add cannot race
		// wg.Wait at counter zero this way.
		s.mu.Lock()
		if !s.running {
			s.mu.Unlock()
			return
		}
		s.wg.Add(1)
		s.mu.Unlock()
		defer s.wg.Done()
		if err := s.dial(lifecycle); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.logger.Warn("ws reconnect attempt failed", "label", s.label, "error", err)
			s.scheduleReconnect()
		}
	})
	s.mu.Unlock()
}

func (s *Socket) stopReconnectTimerLocked() {
	if s.reconnectTimer != nil {
		s.reconnectTimer.Stop()
		s.reconnectTimer = nil
	}
}
