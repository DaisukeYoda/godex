package godex

import (
	"context"
	"time"

	"github.com/DaisukeYoda/godex/decimal"
)

// FundingRateScale is the decimal scale of normalized funding rates. Venues
// report rates at unpredictable native precision (dYdX has been observed
// sending 20 fractional digits); adapters round half away from zero to this
// scale so rates from different venues subtract cleanly.
const FundingRateScale = 8

// USDNotionalScale is the decimal scale of normalized USD notionals in market
// statistics (open interest, volume). Statistics are reference values, not
// order inputs, so cent precision is enough.
const USDNotionalScale = 2

// DefaultMarketEventBuffer is the MarketStream Events channel capacity. Like
// the account stream, when it fills the adapter blocks rather than dropping:
// a consumer acting on a silently stale book would quote against prices that
// no longer exist. The stall eventually surfaces as a venue idle-disconnect
// plus reconnect.
const DefaultMarketEventBuffer = 1024

// FundingRate is a venue's current funding rate observation for one market.
type FundingRate struct {
	VenueID VenueID
	Symbol  Symbol
	// Rate is the funding rate per interval at FundingRateScale, signed the
	// way perp venues quote it: positive means longs pay shorts. What
	// "current" means is the venue's own observation — dYdX reports the
	// predicted rate for the upcoming interval, Lighter the latest settled
	// one — so cross-venue comparisons must account for the different
	// observation points (see the adapters' FundingRate docs).
	Rate decimal.Decimal
	// IntervalHours is the venue's funding interval (1 for hourly venues).
	IntervalHours int
	// NextFundingTime is the next application time, nil when the venue's API
	// does not report one.
	NextFundingTime *time.Time
}

// MarketStats are venue market statistics. Reference values only — never
// order inputs.
type MarketStats struct {
	VenueID VenueID
	Symbol  Symbol
	// OpenInterestUSD is the open interest at USDNotionalScale. Venues that
	// report OI in base-asset units are converted with the venue's own
	// reference price, rounding once at the product.
	OpenInterestUSD decimal.Decimal
	// Volume24hUSD is the 24-hour volume at USDNotionalScale.
	Volume24hUSD decimal.Decimal
}

// BookLevel is one price level of an order book.
type BookLevel struct {
	Price decimal.Decimal
	Size  decimal.Decimal
}

// OrderBook is a normalized full order-book snapshot. Bids are sorted best
// (highest) first, asks best (lowest) first. A book is never emitted crossed;
// see MarketStream.
type OrderBook struct {
	VenueID VenueID
	Symbol  Symbol
	Bids    []BookLevel
	Asks    []BookLevel
	// ReceivedAt is the local receive time of the update that produced this
	// snapshot. Consumers use it for staleness decisions.
	ReceivedAt time.Time
}

// MarketEvent is the sealed union of market-stream events. Consumers
// type-switch over the concrete types below; treat unknown variants in the
// default branch as a programming error (fail fast).
type MarketEvent interface {
	isMarketEvent()
}

// BookSnapshotEvent carries a normalized full book snapshot. Adapters rebuild
// the book internally from the venue's snapshot/delta wire protocol; the
// difference never leaks to consumers.
type BookSnapshotEvent struct {
	Book OrderBook
}

// MarketConnectedEvent reports that the market stream is up and subscribed.
type MarketConnectedEvent struct {
	VenueID VenueID
}

// MarketDisconnectedEvent reports that the market stream is down. Book
// snapshots pause until the next MarketConnectedEvent; consumers must treat
// the last snapshot as stale, not current.
type MarketDisconnectedEvent struct {
	VenueID VenueID
}

func (BookSnapshotEvent) isMarketEvent()       {}
func (MarketConnectedEvent) isMarketEvent()    {}
func (MarketDisconnectedEvent) isMarketEvent() {}

// MarketStream is the normalized market-data streaming contract. Like
// executors, one stream serves one market: N markets are N streams.
//
// Design invariants:
//   - A crossed book is never emitted. Sequence gaps, duplicate sequence
//     numbers, and unparseable payloads abort the connection instead of being
//     guessed at (fail fast); the stream reconnects and resubscribes.
//   - Snapshot/delta reassembly is internal; consumers always receive full
//     snapshots.
//
// Between Start and Close a MarketStream keeps itself alive across
// connection drops: drops emit MarketDisconnectedEvent, reconnects emit
// MarketConnectedEvent and resubscribe.
//
// Event ordering contract (Events):
//   - MarketConnectedEvent and MarketDisconnectedEvent alternate, including
//     across internal reconnects.
//   - BookSnapshotEvent is emitted only between a MarketConnectedEvent and
//     the following MarketDisconnectedEvent.
type MarketStream interface {
	// VenueID identifies the venue this stream observes.
	VenueID() VenueID

	// Start dials the venue and subscribes. A first-connect failure is
	// returned and the reconnect loop is not entered (fail fast).
	Start(ctx context.Context) error

	// Events returns the stream's single event channel. The channel is
	// buffered (DefaultMarketEventBuffer); when it fills the adapter blocks
	// rather than dropping. Consume promptly. The channel is closed only
	// after Close completes.
	Events() <-chan MarketEvent

	// Close tears the stream down and closes the event channel. Close is
	// terminal; observing again means constructing a new stream.
	Close() error
}

// MarketDataClient is the normalized polled market-data contract (REST). Like
// executors, one client serves one market. Methods are safe for concurrent
// use.
type MarketDataClient interface {
	// VenueID identifies the venue this client queries.
	VenueID() VenueID

	// FundingRate returns the venue's current funding rate for the
	// configured market.
	FundingRate(ctx context.Context) (FundingRate, error)

	// MarketStats returns venue statistics for the configured market.
	MarketStats(ctx context.Context) (MarketStats, error)
}
