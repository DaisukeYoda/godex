package smoketest

import (
	"strings"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

func connected() godex.AccountEvent    { return godex.ConnectedEvent{VenueID: fakeVenue} }
func disconnected() godex.AccountEvent { return godex.DisconnectedEvent{VenueID: fakeVenue} }

func position() godex.AccountEvent {
	return godex.PositionEvent{Position: godex.Position{Symbol: "SOL-PERP", Size: decimal.New(0, 3)}}
}

// fillAt builds a fill distinguished only by its time, the finest grain the
// contract check can see.
func fillAt(unixMilli int64) godex.AccountEvent {
	return godex.FillEvent{
		OrderID: "order-1",
		Side:    godex.SideBuy,
		Price:   decimal.MustFromString("80.100", 3),
		Size:    decimal.MustFromString("1.000", 3),
		Time:    time.UnixMilli(unixMilli),
	}
}

func TestCheckContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		events  []godex.AccountEvent
		wantErr string // empty means the stream is contract-clean
	}{{
		name:   "empty stream",
		events: nil,
	}, {
		name:   "one closed connection window",
		events: []godex.AccountEvent{connected(), position(), fillAt(1), disconnected()},
	}, {
		name: "reconnect: windows alternate and fills differ",
		events: []godex.AccountEvent{
			connected(), fillAt(1), disconnected(),
			connected(), fillAt(2), disconnected(),
		},
	}, {
		// A session that ends while still connected is not a violation; the
		// contract constrains order, not termination.
		name:   "stream ends connected",
		events: []godex.AccountEvent{connected(), position()},
	}, {
		name:    "connected twice without a disconnect",
		events:  []godex.AccountEvent{connected(), connected()},
		wantErr: "event 1: connected while already connected",
	}, {
		name:    "disconnected without a connect",
		events:  []godex.AccountEvent{disconnected()},
		wantErr: "event 0: disconnected while not connected",
	}, {
		// The clause #8 violates: state carried by a connection published
		// before that connection was announced.
		name:    "state event before the connect that carries it",
		events:  []godex.AccountEvent{connected(), disconnected(), position(), connected()},
		wantErr: "event 2 (position",
	}, {
		name: "fill replayed after a reconnect",
		events: []godex.AccountEvent{
			connected(), fillAt(1), disconnected(),
			connected(), fillAt(1), disconnected(),
		},
		wantErr: "duplicate fill (events 1 and 4)",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckContract(tc.events)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("CheckContract = %v, want a clean stream", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("CheckContract = nil, want %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("CheckContract = %v, want it to report %q", err, tc.wantErr)
			}
		})
	}
}
