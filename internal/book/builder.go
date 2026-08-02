// Package book implements shared order-book reassembly for snapshot+delta
// market-data feeds. Venue adapters own the interpretation of their delta wire
// format and delegate level application here.
//
// Behavior contract (ported from the reference implementation):
//   - Levels are keyed by the venue's native price string and sorted on
//     output.
//   - Level application uses absolute-size semantics: size is the level's
//     current size, and size zero means the level is absent. Applying zero to
//     a missing level is an idempotent no-op (observed in normal operation
//     when snapshot generation races the stream start).
//   - Prices and sizes convert strictly to the configured scales; precision
//     overflow is an error (a broken scale assumption, fail fast).
//   - A crossed book is observable via IsCrossed. Emitting a crossed book is
//     forbidden — Snapshot returns an error while crossed. Recovery policy
//     (waiting for natural resolution, resubscribing) is the caller's.
//   - Protocol violations (non-positive prices, duplicate snapshot prices)
//     are errors (fail fast).
package book

import (
	"fmt"
	"sort"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// Side selects a book side.
type Side string

// Book sides.
const (
	Bids Side = "bids"
	Asks Side = "asks"
)

// RawLevel is the wire representation shared by snapshot and delta levels:
// price and size as decimal strings.
type RawLevel struct {
	Price string
	Size  string
}

// Builder reassembles one market's book from snapshot and delta updates.
type Builder struct {
	venueID     godex.VenueID
	symbol      godex.Symbol
	venueSymbol string
	priceScale  int
	sizeScale   int
	levels      map[Side]map[string]godex.BookLevel
}

// New builds a Builder for one market.
func New(venueID godex.VenueID, symbol godex.Symbol, venueSymbol string, priceScale, sizeScale int) *Builder {
	return &Builder{
		venueID:     venueID,
		symbol:      symbol,
		venueSymbol: venueSymbol,
		priceScale:  priceScale,
		sizeScale:   sizeScale,
		levels: map[Side]map[string]godex.BookLevel{
			Bids: {},
			Asks: {},
		},
	}
}

// ApplySnapshot replaces the whole book. A duplicate price within one side is
// a protocol violation.
func (b *Builder) ApplySnapshot(bids, asks []RawLevel) error {
	for _, entry := range []struct {
		side Side
		raw  []RawLevel
	}{{Bids, bids}, {Asks, asks}} {
		side, raw := entry.side, entry.raw
		levels := map[string]godex.BookLevel{}
		for _, entry := range raw {
			if _, exists := levels[entry.Price]; exists {
				return fmt.Errorf("%s: %s snapshot has duplicate %s price %s",
					b.venueID, b.venueSymbol, side, entry.Price)
			}
			level, err := b.toLevel(entry.Price, entry.Size)
			if err != nil {
				return err
			}
			levels[entry.Price] = level
		}
		b.levels[side] = levels
	}
	return nil
}

// ApplyLevel applies one absolute-size level update. Size zero removes the
// level (idempotent when absent) and returns nil; otherwise the stored level
// is returned so delta-driven adapters can uncross against it.
func (b *Builder) ApplyLevel(side Side, price, size string) (*godex.BookLevel, error) {
	parsedSize, err := decimal.FromString(size, b.sizeScale)
	if err != nil {
		return nil, fmt.Errorf("%s: %s %s size: %w", b.venueID, b.venueSymbol, side, err)
	}
	if parsedSize.Sign() < 0 {
		return nil, fmt.Errorf("%s: %s %s size must not be negative", b.venueID, b.venueSymbol, side)
	}
	if parsedSize.IsZero() {
		delete(b.levels[side], price)
		return nil, nil
	}
	level, err := b.toLevel(price, size)
	if err != nil {
		return nil, err
	}
	b.levels[side][price] = level
	return &level, nil
}

// RemoveCrossedLevels deletes every opposite-side level the given price
// crosses (meets or beats). On delta feeds the latest update is the freshest
// book state, so a crossed opposite level is a filled level whose removal
// delta has not arrived yet. Whether this interpretation applies is a feed
// property; adapters call it explicitly.
func (b *Builder) RemoveCrossedLevels(side Side, price decimal.Decimal) {
	opposite := Asks
	direction := 1
	if side == Asks {
		opposite = Bids
		direction = -1
	}
	for key, level := range b.levels[opposite] {
		if direction*price.Cmp(level.Price) >= 0 {
			delete(b.levels[opposite], key)
		}
	}
}

// IsCrossed reports whether best bid >= best ask. While crossed, the caller
// must suppress emits.
func (b *Builder) IsCrossed() bool {
	bids := b.sorted(Bids)
	asks := b.sorted(Asks)
	return len(bids) > 0 && len(asks) > 0 && bids[0].Price.Cmp(asks[0].Price) >= 0
}

// Snapshot returns the normalized full book. Returning a crossed book is
// forbidden; callers gate on IsCrossed and treat this error as defensive.
func (b *Builder) Snapshot(receivedAt time.Time) (godex.OrderBook, error) {
	bids := b.sorted(Bids)
	asks := b.sorted(Asks)
	if len(bids) > 0 && len(asks) > 0 && bids[0].Price.Cmp(asks[0].Price) >= 0 {
		return godex.OrderBook{}, fmt.Errorf("%s: %s book is crossed", b.venueID, b.venueSymbol)
	}
	return godex.OrderBook{
		VenueID:    b.venueID,
		Symbol:     b.symbol,
		Bids:       bids,
		Asks:       asks,
		ReceivedAt: receivedAt,
	}, nil
}

func (b *Builder) toLevel(price, size string) (godex.BookLevel, error) {
	parsedPrice, err := decimal.FromString(price, b.priceScale)
	if err != nil {
		return godex.BookLevel{}, fmt.Errorf("%s: %s price: %w", b.venueID, b.venueSymbol, err)
	}
	parsedSize, err := decimal.FromString(size, b.sizeScale)
	if err != nil {
		return godex.BookLevel{}, fmt.Errorf("%s: %s size: %w", b.venueID, b.venueSymbol, err)
	}
	if parsedPrice.Sign() <= 0 {
		return godex.BookLevel{}, fmt.Errorf("%s: %s price must be positive", b.venueID, b.venueSymbol)
	}
	if parsedSize.Sign() <= 0 {
		return godex.BookLevel{}, fmt.Errorf("%s: %s size must be positive", b.venueID, b.venueSymbol)
	}
	return godex.BookLevel{Price: parsedPrice, Size: parsedSize}, nil
}

// sorted returns one side ordered best-first: bids descending, asks ascending.
func (b *Builder) sorted(side Side) []godex.BookLevel {
	levels := make([]godex.BookLevel, 0, len(b.levels[side]))
	for _, level := range b.levels[side] {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool {
		if side == Bids {
			return levels[i].Price.Cmp(levels[j].Price) > 0
		}
		return levels[i].Price.Cmp(levels[j].Price) < 0
	})
	return levels
}
