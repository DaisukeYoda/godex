package ws

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/gorilla/websocket"
)

// testTimeout bounds every wait in this file; tests fail loudly instead of
// hanging.
const testTimeout = 5 * time.Second

func testReconnectConfig() godex.ReconnectConfig {
	return godex.ReconnectConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2,
		IdleTimeout:  300 * time.Millisecond,
	}
}

type recorder struct {
	opens    chan struct{}
	messages chan string
	downs    chan struct{}
	// onMessageErr, when set, is returned for messages equal to its key.
	failOn string
}

func newRecorder() *recorder {
	return &recorder{
		opens:    make(chan struct{}, 16),
		messages: make(chan string, 16),
		downs:    make(chan struct{}, 16),
	}
}

func (r *recorder) handlers() Handlers {
	return Handlers{
		OnOpen: func() error {
			r.opens <- struct{}{}
			return nil
		},
		OnMessage: func(data []byte) error {
			text := string(data)
			if r.failOn != "" && text == r.failOn {
				return errors.New("poison message")
			}
			r.messages <- text
			return nil
		},
		OnDown: func() {
			r.downs <- struct{}{}
		},
	}
}

func waitSignal(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitMessage(t *testing.T, ch chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got message %q, want %q", got, want)
		}
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for message %q", want)
	}
}

// newTestServer runs handler per connection with a zero-based connection
// index. The returned URL uses the ws scheme.
func newTestServer(t *testing.T, handler func(conn *websocket.Conn, index int)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handler(conn, int(count.Add(1))-1)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// serveEcho reads until error so control frames are processed (pings get
// pongs) and echoes every text message back.
func serveEcho(conn *websocket.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if err := conn.WriteMessage(kind, data); err != nil {
			return
		}
	}
}

func TestStartOpenSendStop(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, _ int) {
		serveEcho(conn)
	})
	rec := newRecorder()
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())

	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, rec.opens, "OnOpen")
	if !sock.IsOpen() {
		t.Fatal("expected IsOpen after Start")
	}
	if err := sock.Send("hello"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitMessage(t, rec.messages, "hello")

	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	waitSignal(t, rec.downs, "OnDown")
	if sock.IsOpen() {
		t.Fatal("expected closed after Stop")
	}
	// Stopped sockets refuse a second Stop and sends.
	if err := sock.Stop(); err == nil {
		t.Fatal("expected error for double Stop")
	}
	if err := sock.Send("late"); err == nil {
		t.Fatal("expected error for Send after Stop")
	}
}

func TestFirstConnectFailureIsFailFast(t *testing.T) {
	rec := newRecorder()
	// A closed server: dialing must fail.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	srv.Close()

	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())
	if err := sock.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail")
	}
	// No reconnect loop was entered: a subsequent Start is allowed again.
	select {
	case <-rec.opens:
		t.Fatal("unexpected OnOpen after failed Start")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReconnectAfterServerDrop(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, index int) {
		if index == 0 {
			// First connection drops immediately after a greeting.
			_ = conn.WriteMessage(websocket.TextMessage, []byte("first"))
			_ = conn.Close()
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte("second"))
		serveEcho(conn)
	})
	rec := newRecorder()
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())

	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, rec.opens, "first OnOpen")
	waitMessage(t, rec.messages, "first")
	waitSignal(t, rec.downs, "OnDown after drop")
	// The reconnect path re-runs OnOpen (resubscription) on the new
	// connection.
	waitSignal(t, rec.opens, "second OnOpen")
	waitMessage(t, rec.messages, "second")
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestMessageHandlerErrorAbortsIntoReconnect(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, index int) {
		if index == 0 {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("poison"))
			// Keep serving so only the client-side abort ends the connection.
			serveEcho(conn)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte("clean"))
		serveEcho(conn)
	})
	rec := newRecorder()
	rec.failOn = "poison"
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())

	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, rec.opens, "first OnOpen")
	waitSignal(t, rec.downs, "OnDown after poison message")
	waitSignal(t, rec.opens, "second OnOpen")
	waitMessage(t, rec.messages, "clean")
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestIdleWatchdogDetectsHalfOpen(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, index int) {
		if index == 0 {
			// Half-open simulation: never read, so client pings get no pongs
			// and no close frame ever arrives.
			time.Sleep(testTimeout)
			_ = conn.Close()
			return
		}
		serveEcho(conn)
	})
	rec := newRecorder()
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())

	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, rec.opens, "first OnOpen")
	// The watchdog must terminate the silent connection and reconnect.
	waitSignal(t, rec.downs, "OnDown from idle watchdog")
	waitSignal(t, rec.opens, "OnOpen after watchdog reconnect")
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestAbortForcesResubscribe(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, _ int) {
		serveEcho(conn)
	})
	rec := newRecorder()
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())

	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, rec.opens, "first OnOpen")
	sock.Abort()
	waitSignal(t, rec.downs, "OnDown after Abort")
	waitSignal(t, rec.opens, "OnOpen after Abort")
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStartTwiceFails(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, _ int) {
		serveEcho(conn)
	})
	rec := newRecorder()
	sock := New("test", url, testReconnectConfig(), slog.Default(), rec.handlers())
	if err := sock.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sock.Start(context.Background()); err == nil {
		t.Fatal("expected error for double Start")
	}
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// The server greets immediately after the handshake — before any subscribe,
// as dYdX's connected frame does. Adapters initialize connection-scoped state
// in OnOpen, so no message may be delivered before OnOpen returns; otherwise
// the greeting races the hook (for the dydx market stream, consuming
// message_id 0 before the watermark reset corrupted sequence tracking into a
// spurious reconnect cycle).
func TestOnMessageWaitsForOnOpen(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn, _ int) {
		_ = conn.WriteMessage(websocket.TextMessage, []byte("greeting"))
		serveEcho(conn)
	})
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	rec := newRecorder()
	handlers := rec.handlers()
	handlers.OnOpen = func() error {
		close(openStarted)
		<-releaseOpen
		return nil
	}
	sock := New("test", url, testReconnectConfig(), slog.Default(), handlers)

	// Start blocks in OnOpen now, so drive it from a goroutine.
	started := make(chan error, 1)
	go func() { started <- sock.Start(context.Background()) }()
	waitSignal(t, openStarted, "OnOpen started")
	// While OnOpen is blocked, the greeting must not be delivered.
	select {
	case got := <-rec.messages:
		t.Fatalf("message %q delivered before OnOpen completed", got)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseOpen)
	if err := <-started; err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The buffered greeting is delivered once the read loop runs.
	waitMessage(t, rec.messages, "greeting")
	if err := sock.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
