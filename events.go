package godex

import (
	"time"

	"github.com/DaisukeYoda/godex/decimal"
)

// MarginUsageScale is the decimal scale of normalized margin usage ratios
// (0.6200 = 62%).
const MarginUsageScale = 4

// DefaultAccountEventBuffer is the AccountEvents channel capacity. It absorbs
// transient consumer stalls; when it fills, producers block instead of
// dropping (a dropped fill would silently corrupt position state), which
// eventually stalls the WebSocket read loop and surfaces loudly as a venue
// idle-disconnect plus reconnect.
const DefaultAccountEventBuffer = 1024

// Position is a venue position observation.
type Position struct {
	VenueID VenueID
	Symbol  Symbol
	// Size is signed: long positive, short negative, zero flat.
	Size          decimal.Decimal
	EntryPrice    decimal.Decimal
	UnrealizedPnL decimal.Decimal
	// Time is the venue observation timestamp.
	Time time.Time
}

// AccountEvent is the sealed union of account-stream events. Consumers
// type-switch over the concrete types below; treat unknown variants in the
// default branch as a programming error (fail fast), mirroring strict
// discriminator validation.
type AccountEvent interface {
	isAccountEvent()
}

// FillEvent reports an execution from the authenticated account stream — the
// only source of truth for fills.
type FillEvent struct {
	OrderID OrderID
	Side    Side
	Price   decimal.Decimal
	Size    decimal.Decimal
	Time    time.Time
}

// PositionEvent reports a position observation.
type PositionEvent struct {
	Position Position
}

// MarginEvent reports account margin state.
type MarginEvent struct {
	// UsageRatio is at MarginUsageScale; see ComputeMarginUsage.
	UsageRatio decimal.Decimal
	EquityUSD  decimal.Decimal
	Time       time.Time
}

// OrderRejectedEvent reports a venue rejection — e.g. a post-only order that
// would cross. A normal-path event, not an error.
type OrderRejectedEvent struct {
	OrderID OrderID
	Reason  string
}

// ConnectedEvent reports that the account stream is up and the initial (or
// post-reconnect) snapshot follows.
type ConnectedEvent struct {
	VenueID VenueID
}

// DisconnectedEvent reports that the account stream is down; state events
// pause until the next ConnectedEvent.
type DisconnectedEvent struct {
	VenueID VenueID
}

func (FillEvent) isAccountEvent()          {}
func (PositionEvent) isAccountEvent()      {}
func (MarginEvent) isAccountEvent()        {}
func (OrderRejectedEvent) isAccountEvent() {}
func (ConnectedEvent) isAccountEvent()     {}
func (DisconnectedEvent) isAccountEvent()  {}
