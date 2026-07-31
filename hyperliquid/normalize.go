package hyperliquid

// Pure normalization of Hyperliquid payloads into godex types, plus the
// venue's price quantization rule. I/O — subscriptions, order tracking,
// staleness sequencing — is the executor's responsibility.

import (
	"fmt"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

type normalizeContext struct {
	symbol godex.Symbol
	// coin is the venue-native perp name; entries for other coins are
	// ignored (a non-zero foreign position is an account error).
	coin       string
	receivedAt time.Time
}

// accountSnapshot is a normalized clearinghouse observation.
type accountSnapshot struct {
	position godex.Position
	margin   godex.MarginEvent
	// needsRefresh reports a position the venue cannot actually be in — a
	// non-zero size with no entry price — which the account stream publishes
	// transiently right after a fill. The caller re-reads rather than
	// emitting it.
	needsRefresh bool
}

// priceTick returns the finest increment a price of this magnitude may carry.
// Perp prices hold at most five significant figures and at most
// (6 - szDecimals) decimal places; integer prices are always accepted, so the
// tick never grows past 1.
//
// The significant-figure limit depends on the price itself, so this is not a
// fixed venue tick: it is recomputed per order from the price's decimal
// exponent.
func priceTick(price decimal.Decimal, szDecimals int) (decimal.Decimal, error) {
	if price.Sign() <= 0 {
		return decimal.Decimal{}, fmt.Errorf("hyperliquid: price must be positive: %s", price)
	}
	if szDecimals < 0 || szDecimals > maxPerpPriceDecimals {
		return decimal.Decimal{}, fmt.Errorf("hyperliquid: szDecimals %d is outside 0..%d", szDecimals, maxPerpPriceDecimals)
	}
	// price ∈ [10^exponent, 10^(exponent+1)), derived from the mantissa's
	// digit count rather than a float logarithm.
	exponent := len(price.Mantissa().String()) - 1 - price.Scale()

	decimals := maxPriceSigFigs - 1 - exponent
	if limit := maxPerpPriceDecimals - szDecimals; decimals > limit {
		decimals = limit
	}
	if decimals < 0 {
		decimals = 0
	}
	return decimal.New(1, decimals), nil
}

// maintenanceLeverage returns the lowest max leverage the asset's margin
// schedule permits anywhere on its notional range.
//
// Maintenance margin on Hyperliquid is tiered: a perp advertising 25x drops to
// 5x above $50k of notional, which is five times the margin requirement.
// godex.ExecutionMetadata carries a single fraction, so the only answer that
// cannot overstate liquidation headroom is the strictest tier — being too
// conservative costs position size, while being too permissive costs the
// account.
func maintenanceLeverage(asset *metaAsset, tables map[int]*marginTable) (int, error) {
	table, resolved := tables[*asset.MarginTableID]
	if !resolved {
		// The venue omits its flat default tables from the response and names
		// them by the leverage they cap at. That identity is checkable, so it
		// is checked: an id that disagrees with the entry's own max leverage
		// would describe tiers the adapter cannot see, and inventing them is
		// exactly the guess this refuses to make.
		if *asset.MarginTableID != *asset.MaxLeverage {
			return 0, fmt.Errorf(
				"hyperliquid: perp %s references margin table %d, which the venue did not return and "+
					"whose id does not match its max leverage %d, so its maintenance margin is unknown",
				*asset.Name, *asset.MarginTableID, *asset.MaxLeverage)
		}
		return *asset.MaxLeverage, nil
	}
	lowest := 0
	for _, tier := range *table.MarginTiers {
		if lowest == 0 || *tier.MaxLeverage < lowest {
			lowest = *tier.MaxLeverage
		}
	}
	if lowest <= 0 {
		return 0, fmt.Errorf("hyperliquid: margin table %d for %s has no usable tier",
			*asset.MarginTableID, *asset.Name)
	}
	return lowest, nil
}

// maintenanceMarginFraction converts a max leverage into a plain ratio.
// Maintenance margin is half the initial margin at that leverage, so the
// fraction is 1 / (2 · leverage).
func maintenanceMarginFraction(maxLeverage int) (decimal.Decimal, error) {
	if maxLeverage <= 0 {
		return decimal.Decimal{}, fmt.Errorf("hyperliquid: maxLeverage must be positive, got %d", maxLeverage)
	}
	return decimal.New(1, 0).DivToScale(decimal.New(int64(2*maxLeverage), 0), godex.MarginUsageScale), nil
}

// normalizeAccount turns a clearinghouse snapshot into a position and margin
// observation, rejecting account shapes the adapter does not support.
func normalizeAccount(state *clearinghouseState, ctx normalizeContext) (accountSnapshot, error) {
	var target *positionWire
	for i := range *state.AssetPositions {
		position := (*state.AssetPositions)[i].Position
		if *position.Coin == ctx.coin {
			target = position
			continue
		}
		size, err := decimal.FromDecimalString(*position.Szi)
		if err != nil {
			return accountSnapshot{}, fmt.Errorf("hyperliquid: position %s has malformed szi: %w", *position.Coin, err)
		}
		if !size.IsZero() {
			return accountSnapshot{}, fmt.Errorf(
				"hyperliquid: account has an unsupported non-zero position on %s", *position.Coin)
		}
	}

	margin, err := normalizeMargin(state, ctx)
	if err != nil {
		return accountSnapshot{}, err
	}
	snapshot := accountSnapshot{
		margin: margin,
		position: godex.Position{
			VenueID:       godex.VenueHyperliquid,
			Symbol:        ctx.symbol,
			Size:          decimal.New(0, 0),
			EntryPrice:    decimal.New(0, 0),
			UnrealizedPnL: decimal.New(0, 0),
			Time:          ctx.receivedAt,
		},
	}
	if target == nil {
		// No entry for the coin means flat, which is a complete observation.
		return snapshot, nil
	}

	// Liquidation-headroom math uses whole-account equity, so only cross
	// margin is supported.
	if *target.Leverage.Type != leverageTypeCross {
		return accountSnapshot{}, fmt.Errorf(
			"hyperliquid: %s must use cross margin, got %q", ctx.coin, *target.Leverage.Type)
	}

	size, err := decimal.FromDecimalString(*target.Szi)
	if err != nil {
		return accountSnapshot{}, fmt.Errorf("hyperliquid: position %s has malformed szi: %w", ctx.coin, err)
	}
	unrealizedPnL, err := decimal.FromDecimalString(*target.UnrealizedPnl)
	if err != nil {
		return accountSnapshot{}, fmt.Errorf("hyperliquid: position %s has malformed unrealizedPnl: %w", ctx.coin, err)
	}
	entryPrice := decimal.New(0, 0)
	if target.EntryPx != nil {
		entryPrice, err = decimal.FromDecimalString(*target.EntryPx)
		if err != nil {
			return accountSnapshot{}, fmt.Errorf("hyperliquid: position %s has malformed entryPx: %w", ctx.coin, err)
		}
	}
	// Size at no price is not a state the account can be in; ask for a
	// re-read rather than publishing it.
	if !size.IsZero() && entryPrice.IsZero() {
		snapshot.needsRefresh = true
		return snapshot, nil
	}
	if size.IsZero() {
		// A flat position carries no meaningful entry price or PnL; publish
		// zeros rather than whatever the venue left in the fields.
		return snapshot, nil
	}

	snapshot.position.Size = size
	snapshot.position.EntryPrice = entryPrice
	snapshot.position.UnrealizedPnL = unrealizedPnL
	return snapshot, nil
}

func normalizeMargin(state *clearinghouseState, ctx normalizeContext) (godex.MarginEvent, error) {
	// Usage is measured as the share of equity that is not withdrawable.
	usage, err := godex.ComputeMarginUsage(*state.MarginSummary.AccountValue, *state.Withdrawable)
	if err != nil {
		return godex.MarginEvent{}, fmt.Errorf("hyperliquid: margin summary is malformed: %w", err)
	}
	equity, err := decimal.FromDecimalString(*state.MarginSummary.AccountValue)
	if err != nil {
		return godex.MarginEvent{}, fmt.Errorf("hyperliquid: accountValue is malformed: %w", err)
	}
	return godex.MarginEvent{UsageRatio: usage, EquityUSD: equity, Time: ctx.receivedAt}, nil
}

// normalizeFill converts one streamed execution. Fills on other coins belong
// to an account the adapter does not manage and are skipped; the caller has
// already rejected any non-zero foreign position.
func normalizeFill(fill *wsFill, ctx normalizeContext) (*godex.FillEvent, error) {
	if *fill.Coin != ctx.coin {
		return nil, nil
	}
	price, err := decimal.FromDecimalString(*fill.Px)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: fill has malformed px: %w", err)
	}
	size, err := decimal.FromDecimalString(*fill.Sz)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: fill has malformed sz: %w", err)
	}
	side := godex.SideBuy
	if *fill.Side == sideAsk {
		side = godex.SideSell
	}
	// A fill without a client order id belongs to an order this executor did
	// not place — a manual action, or one from a previous process. It still
	// moved the position, so it is reported, with no order to attribute it
	// to.
	var orderID godex.OrderID
	if fill.Cloid != nil {
		orderID = godex.OrderID(*fill.Cloid)
	}
	return &godex.FillEvent{
		OrderID: orderID,
		Side:    side,
		Price:   price,
		Size:    size,
		Time:    time.UnixMilli(*fill.Time),
	}, nil
}
