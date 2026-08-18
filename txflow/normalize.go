package txflow

// Pure normalization of TxFlow payloads into godex types. I/O —
// subscriptions, order tracking, staleness sequencing — is the executor's
// responsibility.

import (
	"fmt"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

type normalizeContext struct {
	symbol godex.Symbol
	// market is the venue-native perp name ("BTC-USDC"); entries for other
	// markets are ignored (a non-zero foreign position is an account error).
	market     string
	receivedAt time.Time
}

// accountSnapshot is a normalized clearinghouse observation.
type accountSnapshot struct {
	position godex.Position
	margin   godex.MarginEvent
	// needsRefresh reports a position the venue cannot actually be in — a
	// non-zero size with no entry price — which the account state publishes
	// transiently right after a fill. The caller re-reads rather than
	// emitting it.
	needsRefresh bool
}

// assetMeta is the resolved venue metadata for the traded perp.
type assetMeta struct {
	// index is the asset id orders, cancels and per-market queries are keyed
	// by.
	index int
	// sizeStep is the perp's size increment (basePrecision).
	sizeStep decimal.Decimal
	// priceTick is the perp's fixed price increment.
	priceTick decimal.Decimal
}

// resolveAssetMeta finds market in the universe and checks it is tradable on
// this adapter's terms.
func resolveAssetMeta(response *perpMetaResponse, market string) (assetMeta, error) {
	for i := range *response.Universe {
		entry := &(*response.Universe)[i]
		if *entry.Name != market {
			continue
		}
		if entry.Delisted != nil && *entry.Delisted {
			return assetMeta{}, fmt.Errorf("txflow: perp %s is delisted", market)
		}
		if entry.HaltTrading != nil && *entry.HaltTrading {
			return assetMeta{}, fmt.Errorf("txflow: perp %s has trading halted", market)
		}
		if entry.OnlyIsolated != nil && *entry.OnlyIsolated {
			return assetMeta{}, fmt.Errorf("txflow: perp %s is isolated-margin only, which is unsupported", market)
		}
		sizeStep, err := decimal.FromDecimalString(*entry.BasePrecision)
		if err != nil {
			return assetMeta{}, fmt.Errorf("txflow: perp %s has malformed basePrecision: %w", market, err)
		}
		// szDecimals and basePrecision describe the same increment two ways;
		// a disagreement means the adapter does not understand this entry.
		if sizeStep.Sign() <= 0 || sizeStep.Cmp(sizeStepFor(*entry.SzDecimals)) != 0 {
			return assetMeta{}, fmt.Errorf("txflow: perp %s has basePrecision %s, which disagrees with szDecimals %d",
				market, *entry.BasePrecision, *entry.SzDecimals)
		}
		priceTick, err := decimal.FromDecimalString(*entry.PriceTick)
		if err != nil {
			return assetMeta{}, fmt.Errorf("txflow: perp %s has malformed priceTick: %w", market, err)
		}
		if priceTick.Sign() <= 0 {
			return assetMeta{}, fmt.Errorf("txflow: perp %s has non-positive priceTick %s", market, *entry.PriceTick)
		}
		return assetMeta{index: *entry.Index, sizeStep: sizeStep, priceTick: priceTick}, nil
	}
	return assetMeta{}, fmt.Errorf("txflow: perp not found in universe: %s", market)
}

// sizeStepFor is 10^-szDecimals. szDecimals is negative for perps whose size
// increment is a multiple of the base unit (DOGE trades in tens), which
// decimal expresses as an integer step at scale zero.
func sizeStepFor(szDecimals int) decimal.Decimal {
	if szDecimals >= 0 {
		return decimal.New(1, szDecimals)
	}
	step := int64(1)
	for range -szDecimals {
		step *= 10
	}
	return decimal.New(step, 0)
}

// normalizeAccount turns a clearinghouse snapshot into a position and margin
// observation, rejecting account shapes the adapter does not support.
func normalizeAccount(state *clearinghouseState, ctx normalizeContext) (accountSnapshot, error) {
	var target *positionWire
	for i := range *state.AssetPositions {
		position := (*state.AssetPositions)[i].Position
		if *position.Coin == ctx.market {
			target = position
			continue
		}
		size, err := decimal.FromDecimalString(*position.Szi)
		if err != nil {
			return accountSnapshot{}, fmt.Errorf("txflow: position %s has malformed szi: %w", *position.Coin, err)
		}
		if !size.IsZero() {
			return accountSnapshot{}, fmt.Errorf(
				"txflow: account has an unsupported non-zero position on %s", *position.Coin)
		}
	}

	margin, err := normalizeMargin(state, ctx)
	if err != nil {
		return accountSnapshot{}, err
	}
	snapshot := accountSnapshot{
		margin: margin,
		position: godex.Position{
			VenueID:       godex.VenueTxFlow,
			Symbol:        ctx.symbol,
			Size:          decimal.New(0, 0),
			EntryPrice:    decimal.New(0, 0),
			UnrealizedPnL: decimal.New(0, 0),
			Time:          ctx.receivedAt,
		},
	}
	if target == nil {
		// No entry for the market means flat, which is a complete observation.
		return snapshot, nil
	}

	// Liquidation-headroom math uses whole-account equity, so only cross
	// margin is supported.
	if *target.Leverage.Type != leverageTypeCross {
		return accountSnapshot{}, fmt.Errorf(
			"txflow: %s must use cross margin, got %q", ctx.market, *target.Leverage.Type)
	}

	size, err := decimal.FromDecimalString(*target.Szi)
	if err != nil {
		return accountSnapshot{}, fmt.Errorf("txflow: position %s has malformed szi: %w", ctx.market, err)
	}
	unrealizedPnL, err := decimal.FromDecimalString(*target.UnrealizedPnl)
	if err != nil {
		return accountSnapshot{}, fmt.Errorf("txflow: position %s has malformed unrealizedPnl: %w", ctx.market, err)
	}
	entryPrice := decimal.New(0, 0)
	if target.EntryPx != nil {
		entryPrice, err = decimal.FromDecimalString(*target.EntryPx)
		if err != nil {
			return accountSnapshot{}, fmt.Errorf("txflow: position %s has malformed entryPx: %w", ctx.market, err)
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
		return godex.MarginEvent{}, fmt.Errorf("txflow: margin summary is malformed: %w", err)
	}
	equity, err := decimal.FromDecimalString(*state.MarginSummary.AccountValue)
	if err != nil {
		return godex.MarginEvent{}, fmt.Errorf("txflow: accountValue is malformed: %w", err)
	}
	return godex.MarginEvent{UsageRatio: usage, EquityUSD: equity, Time: ctx.receivedAt}, nil
}

// normalizeFill converts one execution. Fills on other markets belong to an
// account the adapter does not manage and are skipped; the caller has already
// rejected any non-zero foreign position. orderID is the executor's id for
// the fill's oid, or empty for an order this executor did not place — a
// manual action, or one from a previous process. It still moved the
// position, so it is reported, with no order to attribute it to.
func normalizeFill(fill *fillWire, orderID godex.OrderID, ctx normalizeContext) (*godex.FillEvent, error) {
	if *fill.Symbol != ctx.market {
		return nil, nil
	}
	price, err := decimal.FromDecimalString(*fill.Px)
	if err != nil {
		return nil, fmt.Errorf("txflow: fill has malformed px: %w", err)
	}
	size, err := decimal.FromDecimalString(*fill.Sz)
	if err != nil {
		return nil, fmt.Errorf("txflow: fill has malformed sz: %w", err)
	}
	side := godex.SideBuy
	if *fill.Side == sideAsk {
		side = godex.SideSell
	}
	return &godex.FillEvent{
		OrderID: orderID,
		Side:    side,
		Price:   price,
		Size:    size,
		Time:    time.UnixMilli(*fill.Time),
	}, nil
}
