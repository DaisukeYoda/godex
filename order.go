package godex

import (
	"time"

	"github.com/DaisukeYoda/godex/decimal"
)

// Symbol is the normalized instrument label, e.g. "SOL-PERP". The symbol
// universe is application configuration, not library contract; adapters map
// it to venue-native market identifiers.
type Symbol string

// Side is the order side.
type Side string

// Order sides.
const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// OrderIntent is the normalized execution intent. There is no GTC: the
// contract supports exactly what a maker/taker strategy needs.
type OrderIntent string

// Order intents.
const (
	// IntentPostOnly quotes maker-only; an order that would cross the book
	// is rejected by the venue (a normal-path outcome).
	IntentPostOnly OrderIntent = "post_only"
	// IntentIOC executes immediately up to the price cap; any remainder is
	// canceled.
	IntentIOC OrderIntent = "ioc"
)

// OrderID is an executor-scoped order identifier. The mapping to venue-native
// IDs is kept inside each adapter.
type OrderID string

// AckStatus is the submission outcome reported by OrderAck.
type AckStatus string

// Ack statuses.
const (
	// AckSubmitted means the venue accepted the submission. It does not mean
	// the order filled.
	AckSubmitted AckStatus = "submitted"
	// AckRejected means the venue (or the adapter's pre-check) rejected the
	// order — e.g. a post-only order that would cross. A normal-path outcome.
	AckRejected AckStatus = "rejected"
)

// NewOrder is a normalized order intent.
type NewOrder struct {
	Symbol Symbol
	Side   Side
	// Price is the desired price before rounding; the executor rounds it to
	// the venue tick (buy floor / sell ceil).
	Price decimal.Decimal
	// Size is the desired size before rounding; the executor quantizes it to
	// the venue step.
	Size       decimal.Decimal
	Intent     OrderIntent
	ReduceOnly bool
}

// OrderAck is the normalized submission acknowledgement.
type OrderAck struct {
	OrderID OrderID
	VenueID VenueID
	Status  AckStatus
	Time    time.Time
}
