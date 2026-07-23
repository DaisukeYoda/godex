package lighter

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
)

var testReceivedAt = time.UnixMilli(1_783_182_000_000)

func testNormalizeContext() normalizeContext {
	return normalizeContext{
		symbol:       "SOL-PERP",
		marketID:     2,
		accountIndex: 48,
		receivedAt:   testReceivedAt,
	}
}

func fixtureAccount(t *testing.T) *restAccount {
	t.Helper()
	var response accountRestResponse
	if err := json.Unmarshal(loadFixture(t, "account_rest.json"), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return &(*response.Accounts)[0]
}

func fixturePayload(t *testing.T, name string) *accountPayload {
	t.Helper()
	message, err := decodeAccountWsMessage(loadFixture(t, name))
	if err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return message.Payload
}

func TestNormalizeAccountSnapshot(t *testing.T) {
	result, err := normalizeAccount(fixtureAccount(t), testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	if result.needsAccountRefresh {
		t.Fatal("complete snapshot must not need a refresh")
	}
	if len(result.events) != 2 {
		t.Fatalf("expected position + margin, got %d events", len(result.events))
	}
	position, ok := result.events[0].(godex.PositionEvent)
	if !ok {
		t.Fatalf("expected PositionEvent, got %T", result.events[0])
	}
	// sign -1 with magnitude "0.050" is a short: -0.050.
	if got := position.Position.Size.String(); got != "-0.050" {
		t.Fatalf("size = %s", got)
	}
	if got := position.Position.EntryPrice.String(); got != "150.300" {
		t.Fatalf("entry = %s", got)
	}
	if got := position.Position.UnrealizedPnL.String(); got != "-0.015" {
		t.Fatalf("pnl = %s", got)
	}
	if position.Position.Time != testReceivedAt {
		t.Fatalf("position ts = %v", position.Position.Time)
	}
	margin, ok := result.events[1].(godex.MarginEvent)
	if !ok {
		t.Fatalf("expected MarginEvent, got %T", result.events[1])
	}
	// (100 - 85) / 100 at MarginUsageScale.
	if got := margin.UsageRatio.String(); got != "0.1500" {
		t.Fatalf("usage = %s", got)
	}
	if got := margin.EquityUSD.String(); got != "99.985000" {
		t.Fatalf("equity = %s", got)
	}
}

func TestNormalizeAccountSynthesizesFlatPosition(t *testing.T) {
	account := fixtureAccount(t)
	empty := []accountPosition{}
	account.Positions = &empty
	result, err := normalizeAccount(account, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	// A missing target-market entry yields a synthetic flat position so
	// consumers converge to flat instead of keeping stale state.
	position, ok := result.events[0].(godex.PositionEvent)
	if !ok || position.Position.Size.Sign() != 0 {
		t.Fatalf("expected flat synthetic position, got %+v", result.events[0])
	}
}

func TestNormalizeAccountRejectsForeignPosition(t *testing.T) {
	account := fixtureAccount(t)
	foreignMarket := int64(9)
	(*account.Positions)[0].MarketID = &foreignMarket
	_, err := normalizeAccount(account, testNormalizeContext())
	if err == nil || !strings.Contains(err.Error(), "unsupported non-zero position") {
		t.Fatalf("expected unsupported-position error, got %v", err)
	}
}

func TestNormalizeAccountRejectsIsolatedMargin(t *testing.T) {
	account := fixtureAccount(t)
	isolated := 1
	(*account.Positions)[0].MarginMode = &isolated
	_, err := normalizeAccount(account, testNormalizeContext())
	if err == nil || !strings.Contains(err.Error(), "cross margin") {
		t.Fatalf("expected cross-margin error, got %v", err)
	}
}

func TestNormalizeUpdateTradeToFill(t *testing.T) {
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_account_update_with_trade.json"), testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	if len(result.events) != 1 {
		t.Fatalf("expected one fill, got %d events", len(result.events))
	}
	fill, ok := result.events[0].(godex.FillEvent)
	if !ok {
		t.Fatalf("expected FillEvent, got %T", result.events[0])
	}
	// Our account (48) is the bid party: a buy matched onto our
	// bid_client_id (the executor-allocated client order index).
	if fill.OrderID != "1783182472088" || fill.Side != godex.SideBuy {
		t.Fatalf("unexpected fill: %+v", fill)
	}
	if fill.Price.String() != "82.236" || fill.Size.String() != "0.200" {
		t.Fatalf("unexpected fill price/size: %+v", fill)
	}
	if fill.Time != time.UnixMilli(1783182477422) {
		t.Fatalf("unexpected fill time: %v", fill.Time)
	}
}

func TestNormalizeUpdateIgnoresTradesWeAreNotPartyTo(t *testing.T) {
	ctx := testNormalizeContext()
	ctx.accountIndex = 999
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_account_update_with_trade.json"), ctx)
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	if len(result.events) != 0 {
		t.Fatalf("expected no events, got %+v", result.events)
	}
}

func TestNormalizeUpdateAskSideIsSell(t *testing.T) {
	ctx := testNormalizeContext()
	ctx.accountIndex = 7 // the ask party in the fixture
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_account_update_with_trade.json"), ctx)
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	fill := result.events[0].(godex.FillEvent)
	if fill.Side != godex.SideSell || fill.OrderID != "178318057992411" {
		t.Fatalf("unexpected ask-side fill: %+v", fill)
	}
}

func TestNormalizeUpdatePosition(t *testing.T) {
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_account_update_with_position.json"), testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	if result.needsAccountRefresh {
		t.Fatal("position with pnl must not need a refresh")
	}
	position, ok := result.events[0].(godex.PositionEvent)
	if !ok || position.Position.Size.Sign() != 0 {
		t.Fatalf("expected flat position event, got %+v", result.events[0])
	}
}

func TestNormalizeUpdatePostOnlyCanceled(t *testing.T) {
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_orders_post_only_canceled.json"), testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	rejected, ok := result.events[0].(godex.OrderRejectedEvent)
	if !ok {
		t.Fatalf("expected OrderRejectedEvent, got %T", result.events[0])
	}
	// The async post-only rejection maps back to the client order index.
	if rejected.OrderID != "1783182277198" || rejected.Reason != orderStatusPostOnlyCanceled {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}
}

func TestNormalizeUpdateMissingPnlNeedsRefresh(t *testing.T) {
	result, err := normalizeAccountUpdate(fixturePayload(t, "ws_account_update_without_pnl.json"), testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	if !result.needsAccountRefresh {
		t.Fatal("missing unrealized_pnl must trigger a REST refresh")
	}
	if len(result.events) != 0 {
		t.Fatalf("incomplete position must not emit events, got %+v", result.events)
	}
}

func TestNormalizeUpdateForeignPositionSetsAccountError(t *testing.T) {
	payload := fixturePayload(t, "ws_account_update_with_trade.json")
	foreignMarket := int64(9)
	sign := 1
	positionSize := "1.000"
	entry := "10.000"
	mode := 0
	payload.Positions = positionsContainer{{
		MarketID:      &foreignMarket,
		Sign:          &sign,
		Position:      &positionSize,
		AvgEntryPrice: &entry,
		MarginMode:    &mode,
	}}
	result, err := normalizeAccountUpdate(payload, testNormalizeContext())
	if err != nil {
		t.Fatalf("normalizeAccountUpdate: %v", err)
	}
	if result.accountErr == nil || !strings.Contains(result.accountErr.Error(), "unsupported non-zero position") {
		t.Fatalf("expected account error, got %v", result.accountErr)
	}
	// Events collected before the failing assertion (the fill) are kept so
	// the caller can apply them before stopping the connection.
	if len(result.events) != 1 {
		t.Fatalf("expected the fill to survive, got %+v", result.events)
	}
}

func TestToEpochTimeHandlesSecondsAndMillis(t *testing.T) {
	if got := toEpochTime(1783182477422); got != time.UnixMilli(1783182477422) {
		t.Fatalf("ms: %v", got)
	}
	if got := toEpochTime(1783182277); got != time.Unix(1783182277, 0) {
		t.Fatalf("s: %v", got)
	}
}
