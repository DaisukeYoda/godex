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

// OrderRejectedEvent reports that an order is finished without having filled
// in full — a post-only order that would cross, an IOC remainder the venue
// cancelled, a short-term order that reached its expiry block. A normal-path
// event, not an error.
//
// It means no further fills are coming for this order. It does not mean the
// order did nothing: an IOC that filled part of its size and had the rest
// cancelled produces both a FillEvent and this. Consumers must treat it as
// closing the order, not as voiding it.
//
// It can also arrive before a fill it accounts for, when the venue reports the
// removal in an earlier message than the execution. Adapters emit fills first
// within a single venue message, but they do not reorder across messages —
// buffering the account stream to tidy this would cost latency on the one
// signal that must not have any. Attribute fills by OrderID rather than
// assuming a rejection is the last word on an order.
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
