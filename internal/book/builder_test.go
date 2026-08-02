package book

import (
	"reflect"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
)

// Ported from the TypeScript reference tests
// (omnibook packages/connectors/src/dydx/order-book-builder.test.ts), which
// exercise the shared VenueOrderBookBuilder through the dydx subclass. The Go
// Builder has no venue delta layer, so applyDelta scenarios are expressed as
// the ApplyLevel / RemoveCrossedLevels calls a delta adapter makes. Each test
// names its TS counterpart in a comment; tests without one are Go additions.

const (
	testVenue       = godex.VenueDydx
	testSymbol      = godex.Symbol("BTC-PERP")
	testVenueSymbol = "BTC-USD"
	// BTC-PERP scales from the reference config: priceScale=1, sizeScale=5.
	testPriceScale = 1
	testSizeScale  = 5
)

// testReceivedAt mirrors RECEIVED_AT (epoch millis) in the TS test.
var testReceivedAt = time.UnixMilli(1_749_600_000_000)

func newBuilder() *Builder {
	return New(testVenue, testSymbol, testVenueSymbol, testPriceScale, testSizeScale)
}

// buildSynced mirrors the TS fixture SUBSCRIBED_BTC_CONTENTS (unsorted input).
func buildSynced(t *testing.T) *Builder {
	t.Helper()
	b := newBuilder()
	if err := b.ApplySnapshot(
		[]RawLevel{
			{Price: "63133", Size: "0.0326"},
			{Price: "63135", Size: "0.0329"},
			{Price: "63131", Size: "0.6338"},
		},
		[]RawLevel{
			{Price: "63136", Size: "0.4724"},
			{Price: "63140", Size: "162.8097"},
		},
	); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	return b
}

// levelStrings mirrors the TS helper of the same name.
func levelStrings(levels []godex.BookLevel) [][2]string {
	out := make([][2]string, 0, len(levels))
	for _, level := range levels {
		out = append(out, [2]string{level.Price.String(), level.Size.String()})
	}
	return out
}

func assertBook(t *testing.T, b *Builder, wantBids, wantAsks [][2]string) godex.OrderBook {
	t.Helper()
	book, err := b.Snapshot(testReceivedAt)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := levelStrings(book.Bids); !reflect.DeepEqual(got, wantBids) {
		t.Fatalf("bids: got %v, want %v", got, wantBids)
	}
	if got := levelStrings(book.Asks); !reflect.DeepEqual(got, wantAsks) {
		t.Fatalf("asks: got %v, want %v", got, wantAsks)
	}
	return book
}

func mustApplyLevel(t *testing.T, b *Builder, side Side, price, size string) *godex.BookLevel {
	t.Helper()
	level, err := b.ApplyLevel(side, price, size)
	if err != nil {
		t.Fatalf("ApplyLevel(%s, %s, %s): %v", side, price, size, err)
	}
	return level
}

var (
	syncedBids = [][2]string{
		{"63135.0", "0.03290"},
		{"63133.0", "0.03260"},
		{"63131.0", "0.63380"},
	}
	syncedAsks = [][2]string{
		{"63136.0", "0.47240"},
		{"63140.0", "162.80970"},
	}
)

// TS: 「未ソートのsnapshotから統一スケールのソート済みフル板を組み立てる」
func TestApplySnapshotSortsAndNormalizes(t *testing.T) {
	book := assertBook(t, buildSynced(t), syncedBids, syncedAsks)
	if book.VenueID != testVenue || book.Symbol != testSymbol {
		t.Fatalf("identity: got %s %s, want %s %s", book.VenueID, book.Symbol, testVenue, testSymbol)
	}
	if !book.ReceivedAt.Equal(testReceivedAt) {
		t.Fatalf("ReceivedAt: got %v, want %v", book.ReceivedAt, testReceivedAt)
	}
}

// TS: 「FailFast: snapshot内の重複価格・0数量・精度超過は例外」(zero/negative price
// と負サイズはGoの追加ケース)
func TestApplySnapshotFailFast(t *testing.T) {
	tests := []struct {
		name string
		bids []RawLevel
	}{
		{"duplicate price", []RawLevel{
			{Price: "63135", Size: "0.1"},
			{Price: "63135", Size: "0.2"},
		}},
		{"zero size", []RawLevel{{Price: "63135", Size: "0"}}},
		{"negative size", []RawLevel{{Price: "63135", Size: "-0.1"}}},
		{"zero price", []RawLevel{{Price: "0", Size: "0.1"}}},
		{"negative price", []RawLevel{{Price: "-63135", Size: "0.1"}}},
		{"price beyond scale", []RawLevel{{Price: "63135.05", Size: "0.1"}}},
		{"size beyond scale", []RawLevel{{Price: "63135", Size: "0.123456"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := newBuilder().ApplySnapshot(tt.bids, nil); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TS: 「差分の更新・追加・削除(size "0")を適用する」
func TestApplyLevelUpdateAddRemove(t *testing.T) {
	b := buildSynced(t)
	updated := mustApplyLevel(t, b, Bids, "63135", "0.0651") // update
	if updated == nil || updated.Price.String() != "63135.0" || updated.Size.String() != "0.06510" {
		t.Fatalf("update must return the stored level, got %v", updated)
	}
	mustApplyLevel(t, b, Bids, "63134", "1") // add
	mustApplyLevel(t, b, Bids, "63131", "0") // remove
	mustApplyLevel(t, b, Asks, "63136", "0") // remove
	assertBook(t, b,
		[][2]string{
			{"63135.0", "0.06510"},
			{"63134.0", "1.00000"},
			{"63133.0", "0.03260"},
		},
		[][2]string{{"63140.0", "162.80970"}},
	)
}

// TS: 「存在しないレベルへのsize "0" は冪等なno-op(絶対値セマンティクス)」
// TS: 「レベル削除(size "0")はuncrossを起こさない」— Goでは削除がレベルを返さない
// (nil, nil)ことで、アダプタがuncrossを呼ばない性質として表れる。
func TestApplyLevelZeroOnAbsentLevelIsNoop(t *testing.T) {
	b := buildSynced(t)
	for _, delta := range []struct {
		side  Side
		price string
	}{
		{Bids, "60000"},
		{Asks, "99999"},
		{Bids, "99999"}, // above every ask; deletion must not trigger anything
	} {
		level, err := b.ApplyLevel(delta.side, delta.price, "0")
		if err != nil || level != nil {
			t.Fatalf("ApplyLevel(%s, %s, 0): got (%v, %v), want (nil, nil)",
				delta.side, delta.price, level, err)
		}
	}
	assertBook(t, b, syncedBids, syncedAsks)
}

// Go addition: a removal is not a way around the scale checks — a price the
// builder would refuse to store is a broken scale assumption either way, and
// accepting it as a silent no-op would hide it.
func TestApplyLevelZeroValidatesPrice(t *testing.T) {
	b := buildSynced(t)
	for _, price := range []string{"63135.05", "0", "-63135", "not-a-price"} {
		if _, err := b.ApplyLevel(Bids, price, "0"); err == nil {
			t.Fatalf("ApplyLevel(%s, 0): expected an error", price)
		}
	}
	assertBook(t, b, syncedBids, syncedAsks)
}

// Go addition: a rejected snapshot leaves the previous book untouched. The
// sides used to be installed one at a time, so a failure on asks stranded the
// book with new bids against old asks.
func TestApplySnapshotRejectionLeavesBookUntouched(t *testing.T) {
	b := buildSynced(t)
	err := b.ApplySnapshot(
		[]RawLevel{{Price: "63200", Size: "1"}},
		[]RawLevel{{Price: "63300", Size: "0.123456"}}, // beyond sizeScale
	)
	if err == nil {
		t.Fatal("expected an error for a size beyond scale")
	}
	assertBook(t, b, syncedBids, syncedAsks)
}

// TS: 「FailFast: 負の数量はConnectorError」
func TestApplyLevelRejectsNegativeSize(t *testing.T) {
	b := buildSynced(t)
	if _, err := b.ApplyLevel(Asks, "63140", "-1"); err == nil {
		t.Fatal("expected error for negative size")
	}
	assertBook(t, b, syncedBids, syncedAsks)
}

// Go addition: precision beyond the configured scales is a broken scale
// assumption (fail fast), matching the snapshot-side TS expectation.
func TestApplyLevelRejectsPrecisionOverflow(t *testing.T) {
	b := buildSynced(t)
	if _, err := b.ApplyLevel(Bids, "63135.05", "0.1"); err == nil {
		t.Fatal("expected error for price beyond scale")
	}
	if _, err := b.ApplyLevel(Bids, "63135", "0.123456"); err == nil {
		t.Fatal("expected error for size beyond scale")
	}
	assertBook(t, b, syncedBids, syncedAsks)
}

// TS: 「uncross: 既存askと交差するbidの適用は、そのbid価格以下のaskを削除する」
func TestRemoveCrossedLevelsFromBid(t *testing.T) {
	b := buildSynced(t)
	level := mustApplyLevel(t, b, Bids, "63136", "0.5") // equals best ask → treated as filled
	if level == nil {
		t.Fatal("ApplyLevel must return the stored level")
	}
	b.RemoveCrossedLevels(Bids, level.Price)
	if b.IsCrossed() {
		t.Fatal("book must not be crossed after uncross")
	}
	assertBook(t, b,
		[][2]string{
			{"63136.0", "0.50000"},
			{"63135.0", "0.03290"},
			{"63133.0", "0.03260"},
			{"63131.0", "0.63380"},
		},
		[][2]string{{"63140.0", "162.80970"}},
	)
}

// TS: 「uncross: 既存bidと交差するaskの適用は、そのask価格以上のbidを削除する」
func TestRemoveCrossedLevelsFromAsk(t *testing.T) {
	b := buildSynced(t)
	level := mustApplyLevel(t, b, Asks, "63133", "1.5") // cuts into the 63135/63133 bids
	if level == nil {
		t.Fatal("ApplyLevel must return the stored level")
	}
	b.RemoveCrossedLevels(Asks, level.Price)
	if b.IsCrossed() {
		t.Fatal("book must not be crossed after uncross")
	}
	assertBook(t, b,
		[][2]string{{"63131.0", "0.63380"}},
		[][2]string{
			{"63133.0", "1.50000"},
			{"63136.0", "0.47240"},
			{"63140.0", "162.80970"},
		},
	)
}

// TS: 「uncross: 同一delta内でbid・askが順に適用されても最終的に交差しない」
func TestRemoveCrossedLevelsSequential(t *testing.T) {
	b := buildSynced(t)
	// bid 63137 removes the existing ask 63136, then ask 63134 removes that bid
	// (latest update wins).
	bid := mustApplyLevel(t, b, Bids, "63137", "0.5")
	b.RemoveCrossedLevels(Bids, bid.Price)
	ask := mustApplyLevel(t, b, Asks, "63134", "1")
	b.RemoveCrossedLevels(Asks, ask.Price)
	if b.IsCrossed() {
		t.Fatal("book must not be crossed after uncross")
	}
	assertBook(t, b,
		[][2]string{
			{"63133.0", "0.03260"},
			{"63131.0", "0.63380"},
		},
		[][2]string{
			{"63134.0", "1.00000"},
			{"63140.0", "162.80970"},
		},
	)
}

// TS: 「交差したsnapshotはuncrossせずisCrossed=trueになり、toVenueOrderBookは
// BookInconsistencyError」
func TestCrossedBookIsObservableAndBlocksSnapshot(t *testing.T) {
	b := newBuilder()
	if err := b.ApplySnapshot(
		[]RawLevel{{Price: "63137", Size: "1"}},
		[]RawLevel{{Price: "63136", Size: "1"}},
	); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if !b.IsCrossed() {
		t.Fatal("IsCrossed must report a crossed snapshot")
	}
	if _, err := b.Snapshot(testReceivedAt); err == nil {
		t.Fatal("Snapshot must fail while crossed")
	}
}

// Go addition: best bid == best ask also counts as crossed (>= comparison).
func TestIsCrossedOnEqualBestPrices(t *testing.T) {
	b := newBuilder()
	if err := b.ApplySnapshot(
		[]RawLevel{{Price: "63136", Size: "1"}},
		[]RawLevel{{Price: "63136", Size: "1"}},
	); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if !b.IsCrossed() {
		t.Fatal("equal best bid/ask must be crossed")
	}
}

// Go addition: an empty book snapshots cleanly with ReceivedAt propagated, and
// a one-sided book is never crossed.
func TestSnapshotEmptyAndOneSidedBook(t *testing.T) {
	b := newBuilder()
	if b.IsCrossed() {
		t.Fatal("empty book must not be crossed")
	}
	book := assertBook(t, b, [][2]string{}, [][2]string{})
	if !book.ReceivedAt.Equal(testReceivedAt) {
		t.Fatalf("ReceivedAt: got %v, want %v", book.ReceivedAt, testReceivedAt)
	}
	mustApplyLevel(t, b, Asks, "63136", "1")
	if b.IsCrossed() {
		t.Fatal("one-sided book must not be crossed")
	}
	assertBook(t, b, [][2]string{}, [][2]string{{"63136.0", "1.00000"}})
}
