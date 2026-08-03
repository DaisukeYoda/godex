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

func fillAt(unixMilli int64) godex.AccountEvent {
	return godex.FillEvent{
		OrderID: "order-1",
		Side:    godex.SideBuy,
		Price:   decimal.MustFromString("80.100", 3),
		Size:    decimal.MustFromString("1.000", 3),
		Time:    time.UnixMilli(unixMilli),
	}
}

// contractCase is checked against both entry points: wantErr is what any
// prefix of a stream must satisfy, wantClosedErr what additionally holds once
// the channel has closed.
type contractCase struct {
	name          string
	events        []godex.AccountEvent
	wantErr       string // empty means CheckContract accepts the stream
	wantClosedErr string // empty means CheckClosedStream accepts it too
}

func TestCheckContract(t *testing.T) {
	for _, tc := range contractCases() {
		t.Run(tc.name, func(t *testing.T) {
			assertCheck(t, "CheckContract", CheckContract(tc.events), tc.wantErr)
		})
	}
}

func TestCheckClosedStream(t *testing.T) {
	for _, tc := range contractCases() {
		t.Run(tc.name, func(t *testing.T) {
			assertCheck(t, "CheckClosedStream", CheckClosedStream(tc.events), tc.wantClosedErr)
		})
	}
}

func contractCases() []contractCase {
	return []contractCase{{
		name:   "empty stream",
		events: nil,
	}, {
		name:   "one closed connection window",
		events: []godex.AccountEvent{connected(), position(), fillAt(1), disconnected()},
	}, {
		name: "reconnect: windows alternate",
		events: []godex.AccountEvent{
			connected(), fillAt(1), disconnected(),
			connected(), fillAt(2), disconnected(),
		},
	}, {
		// The clause Close owes the stream: a session still connected has not
		// ended, but a closed channel that ends connected has lost its final
		// DisconnectedEvent.
		name:          "stream ends connected",
		events:        []godex.AccountEvent{connected(), position()},
		wantClosedErr: "closed while connected",
	}, {
		name:          "connected twice without a disconnect",
		events:        []godex.AccountEvent{connected(), connected()},
		wantErr:       "event 1: connected while already connected",
		wantClosedErr: "event 1: connected while already connected",
	}, {
		name:          "disconnected without a connect",
		events:        []godex.AccountEvent{disconnected()},
		wantErr:       "event 0: disconnected while not connected",
		wantClosedErr: "event 0: disconnected while not connected",
	}, {
		// The clause #8 violates: state carried by a connection published
		// before that connection was announced.
		name:          "state event before the connect that carries it",
		events:        []godex.AccountEvent{connected(), disconnected(), position(), connected()},
		wantErr:       "event 2 (position",
		wantClosedErr: "event 2 (position",
	}, {
		// Two fills alike in every field the event carries. Not a contract
		// violation: without a venue trade ID they are indistinguishable, and
		// a venue may legitimately execute one order in equal slices within a
		// timestamp tick. checkNoDuplicateFills, the Run-only heuristic, is
		// what reports these.
		name: "identical fills in one window",
		events: []godex.AccountEvent{
			connected(), fillAt(1), fillAt(1), disconnected(),
		},
	}}
}

func assertCheck(t *testing.T, name string, err error, wantErr string) {
	t.Helper()
	switch {
	case wantErr == "" && err != nil:
		t.Fatalf("%s = %v, want it to accept the stream", name, err)
	case wantErr != "" && err == nil:
		t.Fatalf("%s = nil, want %q", name, wantErr)
	case wantErr != "" && !strings.Contains(err.Error(), wantErr):
		t.Fatalf("%s = %v, want it to report %q", name, err, wantErr)
	}
}

// The duplicate-fill heuristic is not part of the contract check, so it is
// covered on its own.
func TestCheckNoDuplicateFills(t *testing.T) {
	replayed := []godex.AccountEvent{
		connected(), fillAt(1), disconnected(),
		connected(), fillAt(1), disconnected(),
	}
	err := checkNoDuplicateFills(replayed)
	if err == nil || !strings.Contains(err.Error(), "duplicate fill (events 1 and 4)") {
		t.Fatalf("checkNoDuplicateFills = %v, want the replayed fill reported", err)
	}
	if err := checkNoDuplicateFills([]godex.AccountEvent{connected(), fillAt(1), fillAt(2)}); err != nil {
		t.Fatalf("checkNoDuplicateFills = %v, want fills at distinct times accepted", err)
	}
}
