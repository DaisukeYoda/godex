package lighter

// Pure normalization of Lighter account messages into godex.AccountEvents.
// I/O — subscriptions, order tracking, staleness sequencing — is the
// executor's responsibility.

import (
	"fmt"
	"strconv"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

type normalizeContext struct {
	symbol godex.Symbol
	// Target market ID; entries for other markets are ignored (a non-zero
	// foreign position is an account error).
	marketID     int64
	accountIndex int64
	receivedAt   time.Time
}

type normalizeResult struct {
	events []godex.AccountEvent
	// accountErr reports an invalid account observation (unsupported
	// foreign position, non-cross margin) that arrived alongside
	// transaction events. The caller applies the events first, then stops
	// the connection.
	accountErr error
	// needsAccountRefresh is set when a target-market position lacked
	// unrealized_pnl and a REST refetch is required.
	needsAccountRefresh bool
}

func toEpochTime(timestamp float64) time.Time {
	if timestamp >= epochMsThreshold {
		return time.UnixMilli(int64(timestamp))
	}
	return time.Unix(int64(timestamp), 0)
}

func signedSize(position *accountPosition) (decimal.Decimal, error) {
	parsed, err := decimal.FromDecimalString(*position.Position)
	if err != nil {
		return decimal.Decimal{}, err
	}
	mantissa := parsed.Mantissa()
	mantissa.Abs(mantissa)
	if *position.Sign == -1 {
		mantissa.Neg(mantissa)
	}
	return decimal.FromBigInt(mantissa, parsed.Scale()), nil
}

func assertSupportedPosition(position *accountPosition, ctx normalizeContext) error {
	size, err := decimal.FromDecimalString(*position.Position)
	if err != nil {
		return err
	}
	if *position.MarketID != ctx.marketID && !size.IsZero() {
		return fmt.Errorf("lighter: account has unsupported non-zero position on market %d", *position.MarketID)
	}
	if *position.MarketID == ctx.marketID && *position.MarginMode != marginModeCross {
		return fmt.Errorf("lighter: market %d must use cross margin, got mode %d", *position.MarketID, *position.MarginMode)
	}
	return nil
}

// toPosition returns nil for foreign-market entries and for target entries
// missing unrealized_pnl (those need a REST refetch).
func toPosition(position *accountPosition, ctx normalizeContext) (*godex.Position, error) {
	if *position.MarketID != ctx.marketID || position.UnrealizedPnl == nil {
		return nil, nil
	}
	size, err := signedSize(position)
	if err != nil {
		return nil, err
	}
	entryPrice, err := decimal.FromDecimalString(*position.AvgEntryPrice)
	if err != nil {
		return nil, err
	}
	unrealizedPnl, err := decimal.FromDecimalString(*position.UnrealizedPnl)
	if err != nil {
		return nil, err
	}
	return &godex.Position{
		VenueID:       godex.VenueLighter,
		Symbol:        ctx.symbol,
		Size:          size,
		EntryPrice:    entryPrice,
		UnrealizedPnL: unrealizedPnl,
		Time:          ctx.receivedAt,
	}, nil
}

// toFill matches the trade side where our account is the party and maps its
// client_order_index (allocated at submission) onto the executor OrderID.
// Returns nil for foreign markets and trades we are not a party to.
func toFill(trade *accountTrade, ctx normalizeContext) (*godex.FillEvent, error) {
	if *trade.MarketID != ctx.marketID {
		return nil, nil
	}
	isBid := *trade.BidAccountID == ctx.accountIndex
	var clientOrderIndex int64
	switch {
	case isBid:
		clientOrderIndex = *trade.BidClientID
	case *trade.AskAccountID == ctx.accountIndex:
		clientOrderIndex = *trade.AskClientID
	default:
		return nil, nil
	}
	price, err := decimal.FromDecimalString(*trade.Price)
	if err != nil {
		return nil, err
	}
	size, err := decimal.FromDecimalString(*trade.Size)
	if err != nil {
		return nil, err
	}
	side := godex.SideSell
	if isBid {
		side = godex.SideBuy
	}
	return &godex.FillEvent{
		OrderID: godex.OrderID(strconv.FormatInt(clientOrderIndex, 10)),
		Side:    side,
		Price:   price,
		Size:    size,
		Time:    toEpochTime(*trade.Timestamp),
	}, nil
}

// normalizeAccount converts a REST account snapshot into position + margin
// events. A missing target-market position yields a synthetic flat position.
func normalizeAccount(account *restAccount, ctx normalizeContext) (normalizeResult, error) {
	result := normalizeResult{}
	sawTargetPosition := false
	for i := range *account.Positions {
		position := &(*account.Positions)[i]
		if err := assertSupportedPosition(position, ctx); err != nil {
			return normalizeResult{}, err
		}
		if *position.MarketID == ctx.marketID {
			sawTargetPosition = true
		}
		normalized, err := toPosition(position, ctx)
		if err != nil {
			return normalizeResult{}, err
		}
		switch {
		case normalized != nil:
			result.events = append(result.events, godex.PositionEvent{Position: *normalized})
		case *position.MarketID == ctx.marketID:
			result.needsAccountRefresh = true
		}
	}
	if !sawTargetPosition {
		result.events = append(result.events, godex.PositionEvent{Position: godex.Position{
			VenueID: godex.VenueLighter,
			Symbol:  ctx.symbol,
			Time:    ctx.receivedAt,
		}})
	}
	usageRatio, err := godex.ComputeMarginUsage(*account.Collateral, *account.AvailableBalance)
	if err != nil {
		return normalizeResult{}, err
	}
	equity, err := decimal.FromDecimalString(*account.TotalAssetValue)
	if err != nil {
		return normalizeResult{}, err
	}
	result.events = append(result.events, godex.MarginEvent{
		UsageRatio: usageRatio,
		EquityUSD:  equity,
		Time:       ctx.receivedAt,
	})
	return result, nil
}

// normalizeAccountUpdate converts a WS account payload into fill / rejection /
// position events. An unsupported position sets accountErr while keeping the
// events collected so far, so the caller can apply them before stopping.
func normalizeAccountUpdate(payload *accountPayload, ctx normalizeContext) (normalizeResult, error) {
	result := normalizeResult{}

	// Order updates (account_all_orders): post-only rejection detection. A
	// crossing post-only is accepted by sendTx and rejected asynchronously
	// with this status — a normal-path event.
	for i := range payload.Orders {
		order := &payload.Orders[i]
		if *order.MarketIndex != ctx.marketID {
			continue
		}
		if order.Status != nil && *order.Status == orderStatusPostOnlyCanceled {
			result.events = append(result.events, godex.OrderRejectedEvent{
				OrderID: godex.OrderID(strconv.FormatInt(*order.ClientOrderIndex, 10)),
				Reason:  *order.Status,
			})
		}
	}

	for i := range payload.Trades {
		fill, err := toFill(&payload.Trades[i], ctx)
		if err != nil {
			return normalizeResult{}, err
		}
		if fill != nil {
			result.events = append(result.events, *fill)
		}
	}

	for i := range payload.Positions {
		if err := assertSupportedPosition(&payload.Positions[i], ctx); err != nil {
			result.accountErr = err
			return result, nil
		}
	}
	for i := range payload.Positions {
		position := &payload.Positions[i]
		normalized, err := toPosition(position, ctx)
		if err != nil {
			return normalizeResult{}, err
		}
		switch {
		case normalized != nil:
			result.events = append(result.events, godex.PositionEvent{Position: *normalized})
		case *position.MarketID == ctx.marketID:
			result.needsAccountRefresh = true
		}
	}

	return result, nil
}
