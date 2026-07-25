package dydx

import (
	"fmt"
	"math/big"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// Pure translation between dYdX wire payloads and godex's normalized types. No
// I/O, no clock, no locking — everything here is a function of its arguments.

// normalizeContext carries the executor's fixed labels into normalization.
type normalizeContext struct {
	symbol godex.Symbol
	ticker string
	// receivedAt stamps observations the venue does not timestamp itself.
	receivedAt time.Time
}

// scaleToInteger returns value * 10^exponent as an exact integer, or an error
// when the conversion would lose precision. Used to move an already-rounded
// decimal price or size into the venue's integer wire units.
func scaleToInteger(value decimal.Decimal, exponent int, label string) (*big.Int, error) {
	mantissa := value.Mantissa()
	shift := exponent - value.Scale()
	if shift >= 0 {
		return new(big.Int).Mul(mantissa, pow10(shift)), nil
	}
	quotient, remainder := new(big.Int).QuoRem(mantissa, pow10(-shift), new(big.Int))
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf("dydx: %s %s cannot be represented in venue wire units without loss",
			label, value)
	}
	return quotient, nil
}

func pow10(exponent int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}

// quantizeToMultiple checks that value is an exact multiple of step and returns
// it as a uint64.
//
// The caller has already rounded to the market's published tickSize/stepSize,
// so this conversion must come out even. A remainder means tickSize/stepSize
// disagrees with subticksPerTick/stepBaseQuantums — a venue metadata problem
// that would otherwise be silently absorbed as a mis-priced or mis-sized order.
func quantizeToMultiple(value *big.Int, step int64, label string) (uint64, error) {
	if value.Sign() <= 0 {
		return 0, fmt.Errorf("dydx: %s must be positive, got %s", label, value)
	}
	stepBig := big.NewInt(step)
	if remainder := new(big.Int).Mod(value, stepBig); remainder.Sign() != 0 {
		return 0, fmt.Errorf(
			"dydx: market metadata inconsistency: %s %s is not a multiple of %d",
			label, value, step)
	}
	if !value.IsUint64() {
		return 0, fmt.Errorf("dydx: %s %s exceeds the venue's uint64 wire field", label, value)
	}
	return value.Uint64(), nil
}

// marketMeta is the market's order-placement metadata with every field already
// parsed.
//
// Connect resolves this before it starts the account stream or any poller, so a
// market payload that decodes but carries an unusable decimal fails the
// connection cleanly instead of leaving a half-built executor running.
type marketMeta struct {
	clobPairID                uint32
	tick                      decimal.Decimal
	step                      decimal.Decimal
	maintenanceMarginFraction decimal.Decimal
	atomicResolution          int32
	quantumConversionExponent int32
	stepBaseQuantums          int64
	subticksPerTick           int64
}

// newMarketMeta parses a validated market payload.
func newMarketMeta(market *perpetualMarket) (*marketMeta, error) {
	clobPairID, err := market.clobPairIDValue()
	if err != nil {
		return nil, err
	}
	tick, err := decimal.FromDecimalString(*market.TickSize)
	if err != nil {
		return nil, fmt.Errorf("dydx: market tickSize %q: %w", *market.TickSize, err)
	}
	step, err := decimal.FromDecimalString(*market.StepSize)
	if err != nil {
		return nil, fmt.Errorf("dydx: market stepSize %q: %w", *market.StepSize, err)
	}
	maintenanceMargin, err := decimal.FromDecimalString(*market.MaintenanceMarginFraction)
	if err != nil {
		return nil, fmt.Errorf("dydx: market maintenanceMarginFraction %q: %w",
			*market.MaintenanceMarginFraction, err)
	}
	if tick.Sign() <= 0 || step.Sign() <= 0 {
		return nil, fmt.Errorf("dydx: market tickSize %s and stepSize %s must both be positive",
			tick, step)
	}
	return &marketMeta{
		clobPairID:                clobPairID,
		tick:                      tick,
		step:                      step,
		maintenanceMarginFraction: maintenanceMargin,
		atomicResolution:          *market.AtomicResolution,
		quantumConversionExponent: *market.QuantumConversionExponent,
		stepBaseQuantums:          *market.StepBaseQuantums,
		subticksPerTick:           *market.SubticksPerTick,
	}, nil
}

// toQuantums converts a step-rounded size into base quantums.
func (m *marketMeta) toQuantums(size decimal.Decimal) (uint64, error) {
	scaled, err := scaleToInteger(size, int(-m.atomicResolution), "size")
	if err != nil {
		return 0, err
	}
	return quantizeToMultiple(scaled, m.stepBaseQuantums, "quantums")
}

// toSubticks converts a tick-rounded price into subticks.
//
// The venue's exponent relates the base asset's atomic resolution, the market's
// quantum conversion exponent, and USDC's fixed quote resolution.
func (m *marketMeta) toSubticks(price decimal.Decimal) (uint64, error) {
	exponent := int(m.atomicResolution) - int(m.quantumConversionExponent) - quoteAtomicResolution
	scaled, err := scaleToInteger(price, exponent, "price")
	if err != nil {
		return 0, err
	}
	return quantizeToMultiple(scaled, m.subticksPerTick, "subticks")
}

// toSide maps a normalized side onto the venue's order side enum.
func toSide(side godex.Side) (int32, error) {
	switch side {
	case godex.SideBuy:
		return sideBuy, nil
	case godex.SideSell:
		return sideSell, nil
	default:
		return 0, fmt.Errorf("dydx: invalid side %q", side)
	}
}

// toTimeInForce maps a normalized intent onto the venue's time-in-force enum.
// The contract has no GTC, so every order is post-only or IOC.
func toTimeInForce(intent godex.OrderIntent) (int32, error) {
	switch intent {
	case godex.IntentPostOnly:
		return timeInForcePostOnly, nil
	case godex.IntentIOC:
		return timeInForceIOC, nil
	default:
		return 0, fmt.Errorf("dydx: invalid order intent %q", intent)
	}
}

// toPosition converts a venue position into a normalized one. Size is signed by
// the venue's side field.
func toPosition(entry *perpetualPosition, ctx normalizeContext) (godex.Position, error) {
	size, err := decimal.FromDecimalString(*entry.Size)
	if err != nil {
		return godex.Position{}, fmt.Errorf("dydx: position size: %w", err)
	}
	if *entry.Side == positionSideShort {
		size = decimal.New(0, size.Scale()).Sub(size)
	}
	entryPrice, err := decimal.FromDecimalString(*entry.EntryPrice)
	if err != nil {
		return godex.Position{}, fmt.Errorf("dydx: position entryPrice: %w", err)
	}
	unrealizedPnL, err := decimal.FromDecimalString(*entry.UnrealizedPnl)
	if err != nil {
		return godex.Position{}, fmt.Errorf("dydx: position unrealizedPnl: %w", err)
	}
	return godex.Position{
		VenueID:       godex.VenueDydx,
		Symbol:        ctx.symbol,
		Size:          size,
		EntryPrice:    entryPrice,
		UnrealizedPnL: unrealizedPnL,
		Time:          ctx.receivedAt,
	}, nil
}

// flatPosition is the zero position for the configured market, synthesized when
// the venue reports no open position for it.
func flatPosition(ctx normalizeContext) godex.Position {
	return godex.Position{
		VenueID:       godex.VenueDydx,
		Symbol:        ctx.symbol,
		Size:          decimal.New(0, 0),
		EntryPrice:    decimal.New(0, 0),
		UnrealizedPnL: decimal.New(0, 0),
		Time:          ctx.receivedAt,
	}
}

// findPosition returns the entry for the configured market, if any.
func findPosition(positions positionsContainer, ticker string) *perpetualPosition {
	for i := range positions {
		if *positions[i].Market == ticker {
			return &positions[i]
		}
	}
	return nil
}

// toMargin converts an account snapshot into a margin observation.
func toMargin(account *subaccount, ctx normalizeContext) (godex.MarginEvent, error) {
	usage, err := godex.ComputeMarginUsage(*account.Equity, *account.FreeCollateral)
	if err != nil {
		return godex.MarginEvent{}, fmt.Errorf("dydx: margin usage: %w", err)
	}
	equity, err := decimal.FromDecimalString(*account.Equity)
	if err != nil {
		return godex.MarginEvent{}, fmt.Errorf("dydx: equity: %w", err)
	}
	return godex.MarginEvent{UsageRatio: usage, EquityUSD: equity, Time: ctx.receivedAt}, nil
}

// normalizeSnapshot converts a full account snapshot into the position and
// margin events emitted after every connect and reconnect. A market with no
// open position yields an explicit flat position, so consumers always learn the
// account's state rather than inferring it from silence.
func normalizeSnapshot(account *subaccount, ctx normalizeContext) ([]godex.AccountEvent, error) {
	position := flatPosition(ctx)
	if entry := findPosition(account.OpenPerpetualPositions, ctx.ticker); entry != nil {
		if !entry.complete() {
			return nil, fmt.Errorf(
				"dydx: position snapshot for %s is missing entryPrice or unrealizedPnl", ctx.ticker)
		}
		converted, err := toPosition(entry, ctx)
		if err != nil {
			return nil, err
		}
		position = converted
	}
	margin, err := toMargin(account, ctx)
	if err != nil {
		return nil, err
	}
	return []godex.AccountEvent{godex.PositionEvent{Position: position}, margin}, nil
}

// toFill converts a venue fill into a normalized one. The fill's own venue
// timestamp is used, not the receive time, so a fill re-delivered by a backfill
// is byte-identical to the one that arrived live.
func toFill(entry *fill, orderID godex.OrderID) (godex.FillEvent, error) {
	side := godex.SideBuy
	if *entry.Side == fillSideSell {
		side = godex.SideSell
	}
	price, err := decimal.FromDecimalString(*entry.Price)
	if err != nil {
		return godex.FillEvent{}, fmt.Errorf("dydx: fill price: %w", err)
	}
	size, err := decimal.FromDecimalString(*entry.Size)
	if err != nil {
		return godex.FillEvent{}, fmt.Errorf("dydx: fill size: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339, *entry.CreatedAt)
	if err != nil {
		return godex.FillEvent{}, fmt.Errorf("dydx: fill createdAt %q: %w", *entry.CreatedAt, err)
	}
	return godex.FillEvent{
		OrderID: orderID,
		Side:    side,
		Price:   price,
		Size:    size,
		Time:    createdAt.UTC(),
	}, nil
}

// rejectionReason describes why the venue removed an order, in terms a strategy
// can act on. Short-term orders have no separate expiry event in the contract,
// so reaching good_til_block is reported as a rejection: either way the order is
// gone and will never fill.
func rejectionReason(update *orderUpdate, ref orderRef) string {
	if update.RemovalReason == nil {
		return fmt.Sprintf("venue removed order (status %s)", *update.Status)
	}
	switch *update.RemovalReason {
	case removalReasonExpired, removalReasonIndexerExpired:
		return fmt.Sprintf("expired: good_til_block %d reached", ref.goodTilBlock)
	default:
		return *update.RemovalReason
	}
}

// isRemoval reports whether an order update means the venue took the order off
// the book. BEST_EFFORT_CANCELED is included: the order is gone as far as this
// node knows, and a short-term order cannot come back.
//
// A fully filled order is deliberately excluded — it is terminal too, but it
// ended by executing, and reporting a rejection for it would contradict the
// fills already delivered. isFilled covers that case instead.
func isRemoval(update *orderUpdate) bool {
	switch *update.Status {
	case orderStatusCanceled, orderStatusBestEffortCanceled:
		return true
	default:
		return false
	}
}

// isFilled reports that the order completed by executing in full, so no further
// fill or removal can reference it.
func isFilled(update *orderUpdate) bool {
	return *update.Status == orderStatusFilled
}

// clientIDOf returns an order update's client id.
func clientIDOf(update *orderUpdate) (uint32, error) {
	return parseUint32Field("order update", "clientId", *update.ClientID)
}
