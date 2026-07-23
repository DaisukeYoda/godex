package smoketest

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

const fakeVenue = godex.VenueID("mock")

// fakeExecutor is an in-memory VenueExecutor that behaves like a healthy
// venue; flags introduce specific misbehaviors so tests can prove the gates
// actually detect them.
type fakeExecutor struct {
	events chan godex.AccountEvent
	ask    decimal.Decimal // post-only prices at or above this cross

	// misbehaviors under test
	duplicateFillOnReconnect bool
	acceptCrossingPostOnly   bool

	mu       sync.Mutex
	seq      int
	orders   map[godex.OrderID]bool
	position decimal.Decimal
	lastFill *godex.FillEvent
}

func newFakeExecutor(ask decimal.Decimal) *fakeExecutor {
	return &fakeExecutor{
		events: make(chan godex.AccountEvent, godex.DefaultAccountEventBuffer),
		ask:    ask,
		orders: make(map[godex.OrderID]bool),
	}
}

func (f *fakeExecutor) VenueID() godex.VenueID                   { return fakeVenue }
func (f *fakeExecutor) AccountEvents() <-chan godex.AccountEvent { return f.events }
func (f *fakeExecutor) CancelOrder(_ context.Context, id godex.OrderID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.orders[id] {
		return godex.ErrUnknownOrder
	}
	delete(f.orders, id)
	return nil
}

func (f *fakeExecutor) Connect(context.Context) (godex.ExecutionMetadata, error) {
	f.emitSnapshot()
	return godex.ExecutionMetadata{
		SizeStep:                  decimal.New(1, 3),
		MaintenanceMarginFraction: decimal.MustFromString("0.03", 4),
	}, nil
}

func (f *fakeExecutor) emitSnapshot() {
	f.events <- godex.ConnectedEvent{VenueID: fakeVenue}
	f.mu.Lock()
	position := f.position
	f.mu.Unlock()
	f.events <- godex.PositionEvent{Position: godex.Position{
		VenueID: fakeVenue, Symbol: "SOL-PERP", Size: position, Time: time.Now(),
	}}
	f.events <- godex.MarginEvent{
		UsageRatio: decimal.New(0, godex.MarginUsageScale),
		EquityUSD:  decimal.MustFromString("1000.00", 2),
		Time:       time.Now(),
	}
}

func (f *fakeExecutor) PlaceOrder(_ context.Context, order godex.NewOrder) (godex.OrderAck, error) {
	f.mu.Lock()
	f.seq++
	id := godex.OrderID(strconv.Itoa(f.seq))
	f.mu.Unlock()

	switch order.Intent {
	case godex.IntentPostOnly:
		if !f.acceptCrossingPostOnly && order.Price.Cmp(f.ask) >= 0 {
			f.events <- godex.OrderRejectedEvent{OrderID: id, Reason: "post-only would cross"}
			return godex.OrderAck{OrderID: id, VenueID: fakeVenue, Status: godex.AckRejected, Time: time.Now()}, nil
		}
		f.mu.Lock()
		f.orders[id] = true
		f.mu.Unlock()
		return godex.OrderAck{OrderID: id, VenueID: fakeVenue, Status: godex.AckSubmitted, Time: time.Now()}, nil
	case godex.IntentIOC:
		fill := godex.FillEvent{OrderID: id, Side: order.Side, Price: order.Price, Size: order.Size, Time: time.Now()}
		f.mu.Lock()
		f.lastFill = &fill
		if order.ReduceOnly {
			f.position = decimal.New(0, order.Size.Scale())
		} else {
			f.position = order.Size
		}
		position := f.position
		f.mu.Unlock()
		f.events <- fill
		f.events <- godex.PositionEvent{Position: godex.Position{
			VenueID: fakeVenue, Symbol: order.Symbol, Size: position, EntryPrice: order.Price, Time: time.Now(),
		}}
		return godex.OrderAck{OrderID: id, VenueID: fakeVenue, Status: godex.AckSubmitted, Time: time.Now()}, nil
	default:
		return godex.OrderAck{}, godex.ErrUnknownOrder
	}
}

// forceReconnect simulates the executor's internal reconnect: down, up, and a
// fresh snapshot. The duplicate-fill misbehavior re-emits the last fill, which
// the harness must catch.
func (f *fakeExecutor) forceReconnect() error {
	f.events <- godex.DisconnectedEvent{VenueID: fakeVenue}
	f.emitSnapshot()
	f.mu.Lock()
	lastFill := f.lastFill
	f.mu.Unlock()
	if f.duplicateFillOnReconnect && lastFill != nil {
		f.events <- *lastFill
	}
	return nil
}

func (f *fakeExecutor) Close() error {
	f.events <- godex.DisconnectedEvent{VenueID: fakeVenue}
	close(f.events)
	return nil
}

func testConfig(t *testing.T, fake *fakeExecutor, reconnectCheck bool) Config {
	t.Helper()
	cfg := Config{
		Symbol: "SOL-PERP",
		Size:   decimal.MustFromString("0.200", 3),
		FetchTOB: func(context.Context) (TOB, error) {
			return TOB{
				BestBid: decimal.MustFromString("80.000", 3),
				BestAsk: decimal.MustFromString("80.100", 3),
			}, nil
		},
		Logf:         t.Logf,
		EventTimeout: 500 * time.Millisecond,
		FarOrderRest: time.Millisecond,
	}
	if reconnectCheck {
		cfg.ForceReconnect = fake.forceReconnect
	}
	return cfg
}

func TestRunPassesWithHealthyExecutor(t *testing.T) {
	fake := newFakeExecutor(decimal.MustFromString("80.100", 3))
	if err := Run(context.Background(), fake, testConfig(t, fake, true)); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestRunDetectsDuplicateFillAcrossReconnect(t *testing.T) {
	fake := newFakeExecutor(decimal.MustFromString("80.100", 3))
	fake.duplicateFillOnReconnect = true
	err := Run(context.Background(), fake, testConfig(t, fake, true))
	if err == nil || !strings.Contains(err.Error(), "duplicate fill") {
		t.Fatalf("expected duplicate-fill gate failure, got %v", err)
	}
}

func TestRunDetectsUnrejectedCrossingPostOnly(t *testing.T) {
	fake := newFakeExecutor(decimal.MustFromString("80.100", 3))
	fake.acceptCrossingPostOnly = true
	err := Run(context.Background(), fake, testConfig(t, fake, false))
	if err == nil || !strings.Contains(err.Error(), "order_rejected") {
		t.Fatalf("expected crossing post-only gate failure, got %v", err)
	}
}
