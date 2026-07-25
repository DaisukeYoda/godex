package dydx

import (
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

var testObservedAt = time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC)

func testNormalizeContext() normalizeContext {
	return normalizeContext{symbol: "ETH-PERP", ticker: "ETH-USD", receivedAt: testObservedAt}
}

func testMarket(t *testing.T, ticker string) *marketMeta {
	t.Helper()
	response := decodeFixture[perpetualMarketsResponse](t, "perpetual_markets.json")
	market, err := response.market(ticker)
	if err != nil {
		t.Fatalf("market %s: %v", ticker, err)
	}
	meta, err := newMarketMeta(market)
	if err != nil {
		t.Fatalf("newMarketMeta %s: %v", ticker, err)
	}
	return meta
}

func mustDecimal(t *testing.T, value string) decimal.Decimal {
	t.Helper()
	parsed, err := decimal.FromDecimalString(value)
	if err != nil {
		t.Fatalf("decimal %q: %v", value, err)
	}
	return parsed
}

// TestQuantizationMatchesVenueUnits pins the two conversions an order depends
// on, using real market metadata shapes.
func TestQuantizationMatchesVenueUnits(t *testing.T) {
	eth := testMarket(t, "ETH-USD")
	btc := testMarket(t, "BTC-USD")

	for _, testCase := range []struct {
		name   string
		market *marketMeta
		size   string
		want   uint64
	}{
		// atomicResolution -9: one ETH is 10^9 base quantums.
		{name: "eth one step", market: eth, size: "0.001", want: 1_000_000},
		{name: "eth half unit", market: eth, size: "0.500", want: 500_000_000},
		// atomicResolution -10: one BTC is 10^10 base quantums.
		{name: "btc one step", market: btc, size: "0.0001", want: 1_000_000},
	} {
		t.Run("quantums/"+testCase.name, func(t *testing.T) {
			got, err := testCase.market.toQuantums(mustDecimal(t, testCase.size))
			if err != nil {
				t.Fatalf("toQuantums: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("quantums = %d, want %d", got, testCase.want)
			}
		})
	}

	for _, testCase := range []struct {
		name   string
		market *marketMeta
		price  string
		want   uint64
	}{
		{name: "eth round price", market: eth, price: "3000.0", want: 3_000_000_000},
		{name: "eth one tick", market: eth, price: "0.1", want: 100_000},
		// BTC's exponent differs from ETH's: -10 - (-9) - (-6) = 5.
		{name: "btc round price", market: btc, price: "65000", want: 6_500_000_000},
	} {
		t.Run("subticks/"+testCase.name, func(t *testing.T) {
			got, err := testCase.market.toSubticks(mustDecimal(t, testCase.price))
			if err != nil {
				t.Fatalf("toSubticks: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("subticks = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestQuantizationRejectsUnalignedValues is the safety net against silently
// mis-pricing an order: a value that is not on the venue's grid means the
// caller skipped rounding or the venue's metadata fields disagree.
func TestQuantizationRejectsUnalignedValues(t *testing.T) {
	eth := testMarket(t, "ETH-USD")

	if _, err := eth.toSubticks(mustDecimal(t, "3000.05")); err == nil {
		t.Fatal("expected an error for a price off the tick grid")
	}
	if _, err := eth.toQuantums(mustDecimal(t, "0.0005")); err == nil {
		t.Fatal("expected an error for a size off the step grid")
	}
	if _, err := eth.toQuantums(mustDecimal(t, "0.000")); err == nil {
		t.Fatal("expected an error for a zero size")
	}
}

// TestQuantizationRoundTripsAfterSharedRounding is the composition the executor
// actually performs: round in decimal space with the shared helpers, then
// convert to wire units exactly.
func TestQuantizationRoundTripsAfterSharedRounding(t *testing.T) {
	eth := testMarket(t, "ETH-USD")
	tick := eth.tick
	step := eth.step

	price, err := godex.RoundPriceToTick(mustDecimal(t, "3000.04999"), tick, godex.SideBuy)
	if err != nil {
		t.Fatalf("RoundPriceToTick: %v", err)
	}
	size, err := godex.QuantizeSize(mustDecimal(t, "0.5009"), step, step)
	if err != nil {
		t.Fatalf("QuantizeSize: %v", err)
	}
	subticks, err := eth.toSubticks(price)
	if err != nil {
		t.Fatalf("toSubticks: %v", err)
	}
	quantums, err := eth.toQuantums(size)
	if err != nil {
		t.Fatalf("toQuantums: %v", err)
	}
	// Buy prices floor and sizes floor, so 3000.04999 -> 3000.0 and 0.5009 -> 0.500.
	if subticks != 3_000_000_000 {
		t.Fatalf("subticks = %d, want 3000000000", subticks)
	}
	if quantums != 500_000_000 {
		t.Fatalf("quantums = %d, want 500000000", quantums)
	}
}

func TestToSideAndTimeInForce(t *testing.T) {
	if side, err := toSide(godex.SideBuy); err != nil || side != sideBuy {
		t.Fatalf("toSide(buy) = %d, %v", side, err)
	}
	if side, err := toSide(godex.SideSell); err != nil || side != sideSell {
		t.Fatalf("toSide(sell) = %d, %v", side, err)
	}
	if _, err := toSide("sideways"); err == nil {
		t.Fatal("expected an error for an invalid side")
	}
	if tif, err := toTimeInForce(godex.IntentPostOnly); err != nil || tif != timeInForcePostOnly {
		t.Fatalf("toTimeInForce(post_only) = %d, %v", tif, err)
	}
	if tif, err := toTimeInForce(godex.IntentIOC); err != nil || tif != timeInForceIOC {
		t.Fatalf("toTimeInForce(ioc) = %d, %v", tif, err)
	}
	if _, err := toTimeInForce("gtc"); err == nil {
		t.Fatal("the contract has no GTC; expected an error")
	}
}

func TestNormalizeSnapshotWithPosition(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")
	events, err := normalizeSnapshot(response.Subaccount, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeSnapshot: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want position then margin", len(events))
	}

	position, ok := events[0].(godex.PositionEvent)
	if !ok {
		t.Fatalf("first event is %T, want PositionEvent", events[0])
	}
	if position.Position.VenueID != godex.VenueDydx || position.Position.Symbol != "ETH-PERP" {
		t.Fatalf("position labels = %s/%s", position.Position.VenueID, position.Position.Symbol)
	}
	if position.Position.Size.String() != "0.500" {
		t.Fatalf("size = %s, want 0.500", position.Position.Size)
	}
	if position.Position.EntryPrice.String() != "3000.0" {
		t.Fatalf("entry price = %s", position.Position.EntryPrice)
	}

	margin, ok := events[1].(godex.MarginEvent)
	if !ok {
		t.Fatalf("second event is %T, want MarginEvent", events[1])
	}
	if margin.EquityUSD.String() != "10000.500000" {
		t.Fatalf("equity = %s", margin.EquityUSD)
	}
	// (equity - freeCollateral) / equity = 800.25 / 10000.5 = 0.0800.
	if margin.UsageRatio.String() != "0.0800" {
		t.Fatalf("usage ratio = %s, want 0.0800", margin.UsageRatio)
	}
}

// TestNormalizeSnapshotSynthesizesFlatPosition keeps "no position" explicit:
// consumers learn the account is flat instead of inferring it from silence.
func TestNormalizeSnapshotSynthesizesFlatPosition(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")
	response.Subaccount.OpenPerpetualPositions = nil

	events, err := normalizeSnapshot(response.Subaccount, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeSnapshot: %v", err)
	}
	position := events[0].(godex.PositionEvent).Position
	if position.Size.Sign() != 0 {
		t.Fatalf("size = %s, want flat", position.Size)
	}
	if position.Symbol != "ETH-PERP" {
		t.Fatalf("symbol = %s", position.Symbol)
	}
}

func TestNormalizeSnapshotIgnoresOtherMarkets(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")
	other := "BTC-USD"
	response.Subaccount.OpenPerpetualPositions[0].Market = &other

	events, err := normalizeSnapshot(response.Subaccount, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeSnapshot: %v", err)
	}
	if events[0].(godex.PositionEvent).Position.Size.Sign() != 0 {
		t.Fatal("a position in another market must not become this executor's position")
	}
}

func TestNormalizeSnapshotShortPositionIsNegative(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")
	short := positionSideShort
	response.Subaccount.OpenPerpetualPositions[0].Side = &short

	events, err := normalizeSnapshot(response.Subaccount, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeSnapshot: %v", err)
	}
	if size := events[0].(godex.PositionEvent).Position.Size; size.String() != "-0.500" {
		t.Fatalf("size = %s, want -0.500", size)
	}
}

// TestNormalizeSnapshotRejectsIncompletePosition refuses to publish a position
// missing its priced fields rather than emitting zeros that would read as a
// real entry price.
func TestNormalizeSnapshotRejectsIncompletePosition(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")
	response.Subaccount.OpenPerpetualPositions[0].UnrealizedPnl = nil

	if _, err := normalizeSnapshot(response.Subaccount, testNormalizeContext()); err == nil {
		t.Fatal("expected an error for a position without unrealizedPnl")
	}
}

// TestToFillUsesVenueTimestamp is what lets a backfilled redelivery of a fill
// be recognized as the same event by a consumer.
func TestToFillUsesVenueTimestamp(t *testing.T) {
	response := decodeFixture[fillsResponse](t, "fills.json")
	entry := &(*response.Fills)[0]

	event, err := toFill(entry, "42")
	if err != nil {
		t.Fatalf("toFill: %v", err)
	}
	if event.OrderID != "42" {
		t.Fatalf("order id = %s", event.OrderID)
	}
	if event.Side != godex.SideBuy {
		t.Fatalf("side = %s, want buy", event.Side)
	}
	if event.Price.String() != "3010.2" || event.Size.String() != "0.100" {
		t.Fatalf("price/size = %s/%s", event.Price, event.Size)
	}
	want := time.Date(2026, 7, 25, 11, 5, 3, 421_000_000, time.UTC)
	if !event.Time.Equal(want) {
		t.Fatalf("time = %s, want the venue's createdAt %s", event.Time, want)
	}
}

func TestToFillRejectsMalformedTimestamp(t *testing.T) {
	response := decodeFixture[fillsResponse](t, "fills.json")
	entry := &(*response.Fills)[0]
	bad := "yesterday"
	entry.CreatedAt = &bad

	if _, err := toFill(entry, "42"); err == nil {
		t.Fatal("expected an error for an unparseable createdAt")
	}
}

func TestRejectionReasonExplainsExpiry(t *testing.T) {
	message, err := decodeSubaccountWsMessage(loadFixture(t, "ws_channel_data_order_expired.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	update := &message.Contents.Orders[0]
	ref := orderRef{clientID: 1700000042, goodTilBlock: 38102446}

	reason := rejectionReason(update, ref)
	if reason != "expired: good_til_block 38102446 reached" {
		t.Fatalf("reason = %q", reason)
	}
	if !isRemoval(update) {
		t.Fatal("a CANCELED order must count as removed")
	}
}

func TestRejectionReasonPassesThroughVenueReasons(t *testing.T) {
	status := orderStatusBestEffortCanceled
	reason := "ORDER_REMOVAL_REASON_POST_ONLY_WOULD_CROSS_MAKER_ORDER"
	update := &orderUpdate{Status: &status, RemovalReason: &reason}
	if got := rejectionReason(update, orderRef{}); got != reason {
		t.Fatalf("reason = %q, want the venue's own %q", got, reason)
	}
	if !isRemoval(update) {
		t.Fatal("BEST_EFFORT_CANCELED must count as removed: a short-term order cannot come back")
	}
}

func TestRejectionReasonWithoutVenueReason(t *testing.T) {
	status := orderStatusCanceled
	update := &orderUpdate{Status: &status}
	if got := rejectionReason(update, orderRef{}); got == "" {
		t.Fatal("a removal without a reason still needs a description")
	}
}

func TestIsRemovalIgnoresLiveStatuses(t *testing.T) {
	for _, status := range []string{orderStatusOpen, "BEST_EFFORT_OPENED"} {
		value := status
		if isRemoval(&orderUpdate{Status: &value}) {
			t.Fatalf("status %q must not be treated as a removal", status)
		}
		if isFilled(&orderUpdate{Status: &value}) {
			t.Fatalf("status %q must not be treated as filled", status)
		}
	}
}

// TestFilledIsTerminalButNotARemoval draws the distinction the executor acts
// on: a filled order is finished, so it stops being tracked, but it ended by
// executing — reporting a rejection would contradict the fills already sent.
func TestFilledIsTerminalButNotARemoval(t *testing.T) {
	status := orderStatusFilled
	update := &orderUpdate{Status: &status}
	if isRemoval(update) {
		t.Fatal("a filled order must not produce a rejection")
	}
	if !isFilled(update) {
		t.Fatal("a filled order is terminal and must stop being tracked")
	}
}
