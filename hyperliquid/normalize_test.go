package hyperliquid

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
)

// The venue's price rule is two limits at once — five significant figures and
// at most (6 − szDecimals) decimal places — so the increment depends on the
// price's magnitude rather than being a fixed venue tick.
func TestPriceTick(t *testing.T) {
	tests := []struct {
		name       string
		price      string
		szDecimals int
		want       string
	}{
		{"significant figures bind", "2986.3567", 4, "0.1"},
		{"decimal places bind", "9.87654", 5, "0.1"},
		{"sub-dollar price keeps precision", "0.0012345", 0, "0.000001"},
		{"large price falls back to integers", "123456", 2, "1"},
		{"five figures exactly", "1670.1", 3, "0.1"},
		{"cheap perp", "0.4321", 2, "0.0001"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tick, err := priceTick(mustDecimal(t, test.price), test.szDecimals)
			if err != nil {
				t.Fatalf("priceTick: %v", err)
			}
			if got := tick.String(); got != test.want {
				t.Errorf("priceTick(%s, %d) = %s, want %s", test.price, test.szDecimals, got, test.want)
			}
		})
	}
}

func TestPriceTickRejectsInvalidInput(t *testing.T) {
	if _, err := priceTick(mustDecimal(t, "0"), 2); err == nil {
		t.Error("expected an error for a zero price")
	}
	if _, err := priceTick(mustDecimal(t, "-1"), 2); err == nil {
		t.Error("expected an error for a negative price")
	}
	if _, err := priceTick(mustDecimal(t, "100"), 9); err == nil {
		t.Error("expected an error for out-of-range szDecimals")
	}
}

// Rounding a price to its tick must never produce a price the venue would
// refuse, in either direction.
func TestRoundedPricesSatisfyTheVenueRule(t *testing.T) {
	prices := []string{"2986.3567", "0.0012345", "99999.5", "9.99999", "1.234567", "12345.6"}
	for _, raw := range prices {
		for _, szDecimals := range []int{0, 2, 4, 6} {
			price := mustDecimal(t, raw)
			tick, err := priceTick(price, szDecimals)
			if err != nil {
				t.Fatalf("priceTick(%s, %d): %v", raw, szDecimals, err)
			}
			for _, side := range []godex.Side{godex.SideBuy, godex.SideSell} {
				rounded, err := godex.RoundPriceToTick(price, tick, side)
				if err != nil {
					t.Fatalf("RoundPriceToTick(%s, %s, %s): %v", raw, tick, side, err)
				}
				assertVenuePrice(t, wireDecimal(rounded), szDecimals)
			}
		}
	}
}

// assertVenuePrice checks the wire string against the venue's stated limits.
func assertVenuePrice(t *testing.T, wire string, szDecimals int) {
	t.Helper()
	integer, fraction, hasFraction := splitDecimal(wire)
	if !hasFraction {
		return // Integer prices are always accepted.
	}
	if len(fraction) > maxPerpPriceDecimals-szDecimals {
		t.Errorf("price %s has %d decimals, more than the %d allowed at szDecimals %d",
			wire, len(fraction), maxPerpPriceDecimals-szDecimals, szDecimals)
	}
	digits := integer + fraction
	// Leading zeros are not significant.
	for len(digits) > 0 && digits[0] == '0' {
		digits = digits[1:]
	}
	if len(digits) > maxPriceSigFigs {
		t.Errorf("price %s carries %d significant figures, more than %d", wire, len(digits), maxPriceSigFigs)
	}
}

func splitDecimal(value string) (integer, fraction string, hasFraction bool) {
	for i := range value {
		if value[i] == '.' {
			return value[:i], value[i+1:], true
		}
	}
	return value, "", false
}

func TestMaintenanceMarginFraction(t *testing.T) {
	tests := []struct {
		maxLeverage int
		want        string
	}{
		{50, "0.0100"},
		{20, "0.0250"},
		{3, "0.1667"},
	}
	for _, test := range tests {
		fraction, err := maintenanceMarginFraction(test.maxLeverage)
		if err != nil {
			t.Fatalf("maintenanceMarginFraction(%d): %v", test.maxLeverage, err)
		}
		if got := fraction.String(); got != test.want {
			t.Errorf("maintenanceMarginFraction(%d) = %s, want %s", test.maxLeverage, got, test.want)
		}
	}
	if _, err := maintenanceMarginFraction(0); err == nil {
		t.Error("expected an error for non-positive leverage")
	}
}

func testNormalizeContext() normalizeContext {
	return normalizeContext{
		symbol:     testSymbol,
		coin:       testCoin,
		receivedAt: time.UnixMilli(1753660000000),
	}
}

func decodeClearinghouse(t *testing.T, body []byte) *clearinghouseState {
	t.Helper()
	var state clearinghouseState
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("decode clearinghouseState: %v", err)
	}
	if err := state.validate(); err != nil {
		t.Fatalf("validate clearinghouseState: %v", err)
	}
	return &state
}

func TestNormalizeAccountOpenPosition(t *testing.T) {
	state := decodeClearinghouse(t, loadFixture(t, "clearinghouse_long.json"))
	snapshot, err := normalizeAccount(state, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	if snapshot.needsRefresh {
		t.Fatal("a fully priced position was reported as needing a re-read")
	}
	if got, want := snapshot.position.Size.String(), "0.5000"; got != want {
		t.Errorf("size = %s, want %s", got, want)
	}
	if got, want := snapshot.margin.UsageRatio.String(), "0.0004"; got != want {
		t.Errorf("usage = %s, want %s", got, want)
	}
	if got, want := snapshot.margin.EquityUSD.String(), "13109.482328"; got != want {
		t.Errorf("equity = %s, want %s", got, want)
	}
	if !snapshot.position.Time.Equal(testNormalizeContext().receivedAt) {
		t.Errorf("position time = %s, want the observation time", snapshot.position.Time)
	}
}

func TestNormalizeAccountFlatIsZeroed(t *testing.T) {
	// A flat position can still carry an entry price and PnL the venue never
	// cleared; publishing those would describe a position the account does
	// not hold.
	state := decodeClearinghouse(t, []byte(`{
      "assetPositions":[{"position":{"coin":"ETH","szi":"0.0","entryPx":"2986.3","unrealizedPnl":"12.5",
        "leverage":{"type":"cross","value":20}},"type":"oneWay"}],
      "marginSummary":{"accountValue":"1000","totalMarginUsed":"0"},
      "withdrawable":"400"}`))
	snapshot, err := normalizeAccount(state, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	if !snapshot.position.Size.IsZero() || !snapshot.position.EntryPrice.IsZero() || !snapshot.position.UnrealizedPnL.IsZero() {
		t.Errorf("flat position was not zeroed: %+v", snapshot.position)
	}
	if got, want := snapshot.margin.UsageRatio.String(), "0.6000"; got != want {
		t.Errorf("usage = %s, want %s", got, want)
	}
}

func TestNormalizeAccountAsksForRefreshOnUnpricedPosition(t *testing.T) {
	state := decodeClearinghouse(t, []byte(`{
      "assetPositions":[{"position":{"coin":"ETH","szi":"0.5","entryPx":"0","unrealizedPnl":"0",
        "leverage":{"type":"cross","value":20}},"type":"oneWay"}],
      "marginSummary":{"accountValue":"1000","totalMarginUsed":"0"},
      "withdrawable":"1000"}`))
	snapshot, err := normalizeAccount(state, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	if !snapshot.needsRefresh {
		t.Fatal("a sized position with no entry price should ask for a re-read")
	}
}

func TestNormalizeFill(t *testing.T) {
	ctx := testNormalizeContext()
	tests := []struct {
		name    string
		payload string
		wantNil bool
		side    godex.Side
		orderID godex.OrderID
	}{
		{
			name:    "buy with client order id",
			payload: `{"coin":"ETH","px":"2986.3","sz":"0.5","side":"B","time":1753660000000,"oid":1,"tid":2,"cloid":"0xabc"}`,
			side:    godex.SideBuy,
			orderID: "0xabc",
		},
		{
			name:    "sell without client order id",
			payload: `{"coin":"ETH","px":"2986.3","sz":"0.5","side":"A","time":1753660000000,"oid":1,"tid":2}`,
			side:    godex.SideSell,
		},
		{
			name:    "foreign coin",
			payload: `{"coin":"BTC","px":"60000","sz":"0.1","side":"B","time":1753660000000,"oid":1,"tid":2}`,
			wantNil: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var fill wsFill
			if err := json.Unmarshal([]byte(test.payload), &fill); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := fill.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			event, err := normalizeFill(&fill, ctx)
			if err != nil {
				t.Fatalf("normalizeFill: %v", err)
			}
			if test.wantNil {
				if event != nil {
					t.Fatalf("expected a foreign-coin fill to be skipped, got %+v", event)
				}
				return
			}
			if event == nil {
				t.Fatal("expected a fill event")
			}
			if event.Side != test.side {
				t.Errorf("side = %s, want %s", event.Side, test.side)
			}
			if event.OrderID != test.orderID {
				t.Errorf("order id = %q, want %q", event.OrderID, test.orderID)
			}
			if !event.Time.Equal(time.UnixMilli(1753660000000)) {
				t.Errorf("time = %s, want the venue's fill time", event.Time)
			}
		})
	}
}

func TestFillCacheDeduplicates(t *testing.T) {
	cache := newFillCache()
	if !cache.observe(1) {
		t.Fatal("first observation should be new")
	}
	if cache.observe(1) {
		t.Fatal("a repeated trade id must not be reported as new")
	}
	// Overflow the ring; the newest ids must still be remembered.
	for i := range fillCacheSize + 10 {
		cache.observe(int64(1000 + i))
	}
	if cache.observe(int64(1000 + fillCacheSize + 9)) {
		t.Fatal("the most recent trade id was forgotten")
	}
}

func testMarginTables(t *testing.T) map[int]*marginTable {
	t.Helper()
	var meta metaResponse
	if err := json.Unmarshal(loadFixture(t, "meta.json"), &meta); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	if err := meta.validate(); err != nil {
		t.Fatalf("validate meta: %v", err)
	}
	return meta.marginTablesByID()
}

func metaAssetNamed(t *testing.T, name string) *metaAsset {
	t.Helper()
	var meta metaResponse
	if err := json.Unmarshal(loadFixture(t, "meta.json"), &meta); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	for i := range *meta.Universe {
		if *(*meta.Universe)[i].Name == name {
			return &(*meta.Universe)[i]
		}
	}
	t.Fatalf("%s is not in the fixture universe", name)
	return nil
}

// Maintenance margin is tiered. The contract carries one fraction, so it has
// to be the strictest tier: too conservative costs position size, too
// permissive costs the account.
func TestMaintenanceLeverageUsesStrictestTier(t *testing.T) {
	tables := testMarginTables(t)
	tests := []struct {
		coin string
		want int
	}{
		{"ETH", 5},  // 25x, 10x above $20k, 5x above $50k
		{"BTC", 25}, // 40x, 25x above $10k
		{"SOL", 20}, // no table returned; the flat default at its max leverage
	}
	for _, test := range tests {
		t.Run(test.coin, func(t *testing.T) {
			leverage, err := maintenanceLeverage(metaAssetNamed(t, test.coin), tables)
			if err != nil {
				t.Fatalf("maintenanceLeverage: %v", err)
			}
			if leverage != test.want {
				t.Errorf("maintenanceLeverage = %d, want %d", leverage, test.want)
			}
		})
	}
}

// A missing table is only safe to read as the venue's flat default when its id
// says so. Anything else describes tiers the adapter cannot see, and guessing
// them would overstate liquidation headroom.
func TestMaintenanceLeverageRejectsUnresolvableTable(t *testing.T) {
	_, err := maintenanceLeverage(metaAssetNamed(t, "ODD"), testMarginTables(t))
	if err == nil || !strings.Contains(err.Error(), "maintenance margin is unknown") {
		t.Fatalf("error = %v, want an unresolvable-table error", err)
	}
}
