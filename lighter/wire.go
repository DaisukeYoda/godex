package lighter

// Strict decoding of Lighter REST/WS payloads. Policy: unknown fields are
// tolerated (payloads carry many extras), but missing or mistyped required
// fields and unknown WS discriminators are errors — the connection is aborted
// instead of guessing (fail fast). Required-ness is enforced with pointer
// fields plus validate methods. Shapes were fixed from testnet recordings
// (2026-07-05) of the reference implementation.

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func missingField(object, field string) error {
	return fmt.Errorf("lighter: %s is missing required field %q", object, field)
}

type fieldCheck struct {
	name    string
	present bool
}

func checkRequired(object string, checks ...fieldCheck) error {
	for _, check := range checks {
		if !check.present {
			return missingField(object, check.name)
		}
	}
	return nil
}

func checkResponseCode(object string, code *int) error {
	if code == nil {
		return missingField(object, "code")
	}
	if *code != restSuccessCode {
		return fmt.Errorf("lighter: %s returned code %d", object, *code)
	}
	return nil
}

// orderBookDetail is the order-placement metadata subset of
// GET /api/v1/orderBookDetails.
type orderBookDetail struct {
	MarketID *int64  `json:"market_id"`
	Symbol   *string `json:"symbol"`
	Status   *string `json:"status"`
	// Integer scales (10^-n) for order price and size.
	SupportedPriceDecimals *int `json:"supported_price_decimals"`
	SupportedSizeDecimals  *int `json:"supported_size_decimals"`
	// Minimum size in the base asset (e.g. "0.050"); makers only.
	MinBaseAmount *string `json:"min_base_amount"`
	// Minimum notional in USDC (e.g. "10"); makers only.
	MinQuoteAmount *string `json:"min_quote_amount"`
	// Maintenance margin fraction as an integer in 1/10000ths (240 = 2.40%).
	MaintenanceMarginFraction *int64 `json:"maintenance_margin_fraction"`
}

func (d *orderBookDetail) validate() error {
	const object = "order_book_details entry"
	if err := checkRequired(object,
		fieldCheck{"market_id", d.MarketID != nil},
		fieldCheck{"symbol", d.Symbol != nil},
		fieldCheck{"status", d.Status != nil},
		fieldCheck{"supported_price_decimals", d.SupportedPriceDecimals != nil},
		fieldCheck{"supported_size_decimals", d.SupportedSizeDecimals != nil},
		fieldCheck{"min_base_amount", d.MinBaseAmount != nil},
		fieldCheck{"min_quote_amount", d.MinQuoteAmount != nil},
		fieldCheck{"maintenance_margin_fraction", d.MaintenanceMarginFraction != nil},
	); err != nil {
		return err
	}
	if *d.SupportedPriceDecimals < 0 || *d.SupportedSizeDecimals < 0 {
		return fmt.Errorf("lighter: %s has negative decimals", object)
	}
	if *d.MaintenanceMarginFraction < 0 {
		return fmt.Errorf("lighter: %s has negative maintenance_margin_fraction", object)
	}
	return nil
}

type orderBookDetailsResponse struct {
	Code             *int               `json:"code"`
	OrderBookDetails *[]orderBookDetail `json:"order_book_details"`
}

func (r *orderBookDetailsResponse) validate() error {
	if err := checkResponseCode("orderBookDetails", r.Code); err != nil {
		return err
	}
	if r.OrderBookDetails == nil {
		return missingField("orderBookDetails", "order_book_details")
	}
	for i := range *r.OrderBookDetails {
		if err := (*r.OrderBookDetails)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// nextNonceResponse is GET /api/v1/nextNonce?account_index=&api_key_index=.
type nextNonceResponse struct {
	Code  *int   `json:"code"`
	Nonce *int64 `json:"nonce"`
}

func (r *nextNonceResponse) validate() error {
	if err := checkResponseCode("nextNonce", r.Code); err != nil {
		return err
	}
	if r.Nonce == nil {
		return missingField("nextNonce", "nonce")
	}
	if *r.Nonce < 0 {
		return fmt.Errorf("lighter: nextNonce returned negative nonce %d", *r.Nonce)
	}
	return nil
}

// sendTxResponse is POST /api/v1/sendTx. Errors also arrive as a JSON body
// (code + message), so the code is judged by the caller, not fixed here.
type sendTxResponse struct {
	Code    *int    `json:"code"`
	Message *string `json:"message"`
}

func (r *sendTxResponse) validate() error {
	if r.Code == nil {
		return missingField("sendTx response", "code")
	}
	return nil
}

// accountPosition is shared by REST /api/v1/account and WS account_all.
type accountPosition struct {
	MarketID *int64 `json:"market_id"`
	// 1 = long / -1 = short.
	Sign *int `json:"sign"`
	// Absolute size as a decimal string.
	Position      *string `json:"position"`
	AvgEntryPrice *string `json:"avg_entry_price"`
	// May be absent on WS deltas; triggers a REST refetch.
	UnrealizedPnl *string `json:"unrealized_pnl"`
	// 0 = cross.
	MarginMode *int `json:"margin_mode"`
}

func (p *accountPosition) validate() error {
	const object = "account position"
	if err := checkRequired(object,
		fieldCheck{"market_id", p.MarketID != nil},
		fieldCheck{"sign", p.Sign != nil},
		fieldCheck{"position", p.Position != nil},
		fieldCheck{"avg_entry_price", p.AvgEntryPrice != nil},
		fieldCheck{"margin_mode", p.MarginMode != nil},
	); err != nil {
		return err
	}
	if *p.Sign != 1 && *p.Sign != -1 {
		return fmt.Errorf("lighter: %s has invalid sign %d", object, *p.Sign)
	}
	return nil
}

// restAccount is one entry of GET /api/v1/account?by=index.
type restAccount struct {
	Collateral       *string `json:"collateral"`
	AvailableBalance *string `json:"available_balance"`
	// Account equity including unrealized PnL.
	TotalAssetValue *string            `json:"total_asset_value"`
	Positions       *[]accountPosition `json:"positions"`
}

func (a *restAccount) validate() error {
	const object = "account"
	if err := checkRequired(object,
		fieldCheck{"collateral", a.Collateral != nil},
		fieldCheck{"available_balance", a.AvailableBalance != nil},
		fieldCheck{"total_asset_value", a.TotalAssetValue != nil},
		fieldCheck{"positions", a.Positions != nil},
	); err != nil {
		return err
	}
	for i := range *a.Positions {
		if err := (*a.Positions)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

type accountRestResponse struct {
	Code     *int           `json:"code"`
	Accounts *[]restAccount `json:"accounts"`
}

func (r *accountRestResponse) validate() error {
	if err := checkResponseCode("account", r.Code); err != nil {
		return err
	}
	if r.Accounts == nil || len(*r.Accounts) == 0 {
		return fmt.Errorf("lighter: account response has no accounts")
	}
	for i := range *r.Accounts {
		if err := (*r.Accounts)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// accountTrade is a WS account_all trade. Both parties' client_order_index
// values are included, so fills map onto executor OrderIDs without an
// exchange-index table.
type accountTrade struct {
	MarketID     *int64  `json:"market_id"`
	Size         *string `json:"size"`
	Price        *string `json:"price"`
	AskClientID  *int64  `json:"ask_client_id"`
	BidClientID  *int64  `json:"bid_client_id"`
	AskAccountID *int64  `json:"ask_account_id"`
	BidAccountID *int64  `json:"bid_account_id"`
	// Epoch ms as observed; epoch seconds tolerated defensively.
	Timestamp *float64 `json:"timestamp"`
}

func (t *accountTrade) validate() error {
	return checkRequired("account trade",
		fieldCheck{"market_id", t.MarketID != nil},
		fieldCheck{"size", t.Size != nil},
		fieldCheck{"price", t.Price != nil},
		fieldCheck{"ask_client_id", t.AskClientID != nil},
		fieldCheck{"bid_client_id", t.BidClientID != nil},
		fieldCheck{"ask_account_id", t.AskAccountID != nil},
		fieldCheck{"bid_account_id", t.BidAccountID != nil},
		fieldCheck{"timestamp", t.Timestamp != nil},
	)
}

// orderUpdate is a WS account_all_orders entry; used to observe order status
// (post-only rejections arrive here).
type orderUpdate struct {
	MarketIndex      *int64  `json:"market_index"`
	OrderIndex       *int64  `json:"order_index"`
	ClientOrderIndex *int64  `json:"client_order_index"`
	Status           *string `json:"status"`
}

func (o *orderUpdate) validate() error {
	return checkRequired("order update",
		fieldCheck{"market_index", o.MarketIndex != nil},
		fieldCheck{"order_index", o.OrderIndex != nil},
		fieldCheck{"client_order_index", o.ClientOrderIndex != nil},
	)
}

// The WS account channels use market_id-keyed maps where REST uses arrays;
// the container types accept both and flatten to a slice.

func isJSONArray(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '['
}

type tradesContainer []accountTrade

func (c *tradesContainer) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, (*[]accountTrade)(c))
	}
	var byMarket map[string][]accountTrade
	if err := json.Unmarshal(data, &byMarket); err != nil {
		return err
	}
	*c = nil
	for _, trades := range byMarket {
		*c = append(*c, trades...)
	}
	return nil
}

type positionsContainer []accountPosition

func (c *positionsContainer) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, (*[]accountPosition)(c))
	}
	var byMarket map[string]accountPosition
	if err := json.Unmarshal(data, &byMarket); err != nil {
		return err
	}
	*c = nil
	for _, position := range byMarket {
		*c = append(*c, position)
	}
	return nil
}

type ordersContainer []orderUpdate

func (c *ordersContainer) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, (*[]orderUpdate)(c))
	}
	var byMarket map[string][]orderUpdate
	if err := json.Unmarshal(data, &byMarket); err != nil {
		return err
	}
	*c = nil
	for _, orders := range byMarket {
		*c = append(*c, orders...)
	}
	return nil
}

// accountPayload is the body of the four account WS message types.
type accountPayload struct {
	Channel   *string            `json:"channel"`
	Trades    tradesContainer    `json:"trades"`
	Positions positionsContainer `json:"positions"`
	Orders    ordersContainer    `json:"orders"`
}

func (p *accountPayload) validate() error {
	if p.Channel == nil || *p.Channel == "" {
		return missingField("account ws payload", "channel")
	}
	for i := range p.Trades {
		if err := p.Trades[i].validate(); err != nil {
			return err
		}
	}
	for i := range p.Positions {
		if err := p.Positions[i].validate(); err != nil {
			return err
		}
	}
	for i := range p.Orders {
		if err := p.Orders[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// Account WS message discriminators.
const (
	wsTypeConnected = "connected"
	// Reply to a client ping.
	wsTypePong = "pong"
	// Server-initiated ping; the client must reply with a pong.
	wsTypePing                       = "ping"
	wsTypeSubscribedAccountAll       = "subscribed/account_all"
	wsTypeUpdateAccountAll           = "update/account_all"
	wsTypeSubscribedAccountAllOrders = "subscribed/account_all_orders"
	wsTypeUpdateAccountAllOrders     = "update/account_all_orders"
)

type wsMessage struct {
	Type string
	// Payload is set for the four account payload types.
	Payload *accountPayload
}

// decodeAccountWsMessage parses one account WS frame. Unknown discriminators
// are errors: the caller aborts the connection rather than guessing.
func decodeAccountWsMessage(raw []byte) (wsMessage, error) {
	var envelope struct {
		Type *string `json:"type"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return wsMessage{}, fmt.Errorf("lighter: malformed account ws message: %w", err)
	}
	if envelope.Type == nil {
		return wsMessage{}, missingField("account ws message", "type")
	}
	switch *envelope.Type {
	case wsTypeConnected, wsTypePong, wsTypePing:
		return wsMessage{Type: *envelope.Type}, nil
	case wsTypeSubscribedAccountAll, wsTypeUpdateAccountAll,
		wsTypeSubscribedAccountAllOrders, wsTypeUpdateAccountAllOrders:
		var payload accountPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return wsMessage{}, fmt.Errorf("lighter: malformed %s payload: %w", *envelope.Type, err)
		}
		if err := payload.validate(); err != nil {
			return wsMessage{}, err
		}
		return wsMessage{Type: *envelope.Type, Payload: &payload}, nil
	default:
		return wsMessage{}, fmt.Errorf("lighter: unknown account ws message type %q", *envelope.Type)
	}
}
