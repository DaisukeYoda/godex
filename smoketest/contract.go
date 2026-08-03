package smoketest

import (
	"fmt"

	"github.com/DaisukeYoda/godex"
)

// CheckContract verifies a completed account event stream against the
// AccountEvents ordering contract documented on godex.VenueExecutor:
// ConnectedEvent and DisconnectedEvent alternate, every other event sits
// between a ConnectedEvent and the following DisconnectedEvent, and no fill is
// delivered twice.
//
// It is stream-wide and order-sensitive, so it belongs at the end of a session:
// the live smoketest run calls it after its last gate, and adapter tests can
// call it from a t.Cleanup once their executor is closed and the event channel
// is drained.
func CheckContract(events []godex.AccountEvent) error {
	if err := checkConnectionAlternation(events); err != nil {
		return err
	}
	return checkNoDuplicateFills(events)
}

// checkConnectionAlternation verifies the AccountEvents ordering contract:
// connected/disconnected alternate and every other event sits inside a
// connected window.
func checkConnectionAlternation(events []godex.AccountEvent) error {
	connected := false
	for i, event := range events {
		switch event.(type) {
		case godex.ConnectedEvent:
			if connected {
				return fmt.Errorf("smoketest: event %d: connected while already connected", i)
			}
			connected = true
		case godex.DisconnectedEvent:
			if !connected {
				return fmt.Errorf("smoketest: event %d: disconnected while not connected", i)
			}
			connected = false
		default:
			if !connected {
				return fmt.Errorf("smoketest: event %d (%s) emitted outside a connected window", i, FormatEvent(event))
			}
		}
	}
	return nil
}

// checkNoDuplicateFills verifies no fill was delivered twice (the reconnect
// path must not re-emit executions).
func checkNoDuplicateFills(events []godex.AccountEvent) error {
	seen := make(map[string]int)
	for i, event := range events {
		fill, ok := event.(godex.FillEvent)
		if !ok {
			continue
		}
		key := fmt.Sprintf("%s|%d|%s|%s|%s", fill.OrderID, fill.Time.UnixNano(), fill.Side, fill.Price, fill.Size)
		if first, dup := seen[key]; dup {
			return fmt.Errorf("smoketest: duplicate fill (events %d and %d): %s", first, i, FormatEvent(fill))
		}
		seen[key] = i
	}
	return nil
}
