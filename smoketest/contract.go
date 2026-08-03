package smoketest

import (
	"fmt"

	"github.com/DaisukeYoda/godex"
)

// CheckContract verifies an account event stream against the AccountEvents
// ordering contract documented on godex.VenueExecutor: ConnectedEvent and
// DisconnectedEvent alternate, and every other event sits between a
// ConnectedEvent and the following DisconnectedEvent.
//
// It holds of any prefix of a stream, so it may be called while the executor
// is still running. A stream ending inside a connected window passes here —
// the session simply has not ended yet. Once the event channel has closed that
// is no longer true; use CheckClosedStream.
func CheckContract(events []godex.AccountEvent) error {
	_, err := checkConnectionAlternation(events)
	return err
}

// CheckClosedStream is CheckContract for a stream whose channel has closed,
// which per godex.VenueExecutor happens only after Close. Close emits a final
// DisconnectedEvent if connected, so a terminated stream must not end inside a
// connected window: if it does, either that event was never emitted or events
// were lost on the way out.
//
// Callers must have observed the close — a completed range over the channel —
// not merely called Close. The two are ordered but not simultaneous.
func CheckClosedStream(events []godex.AccountEvent) error {
	connected, err := checkConnectionAlternation(events)
	if err != nil {
		return err
	}
	if connected {
		return fmt.Errorf("smoketest: stream of %d events closed while connected: no final disconnected event", len(events))
	}
	return nil
}

// checkConnectionAlternation verifies the ordering clause, also reporting
// whether the stream ends inside a connected window.
func checkConnectionAlternation(events []godex.AccountEvent) (bool, error) {
	connected := false
	for i, event := range events {
		switch event.(type) {
		case godex.ConnectedEvent:
			if connected {
				return connected, fmt.Errorf("smoketest: event %d: connected while already connected", i)
			}
			connected = true
		case godex.DisconnectedEvent:
			if !connected {
				return connected, fmt.Errorf("smoketest: event %d: disconnected while not connected", i)
			}
			connected = false
		default:
			if !connected {
				return connected, fmt.Errorf("smoketest: event %d (%s) emitted outside a connected window", i, FormatEvent(event))
			}
		}
	}
	return connected, nil
}
