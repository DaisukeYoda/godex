package dydx

// Strict decoding of dYdX Indexer REST/WebSocket and CometBFT RPC payloads.
// Policy matches the rest of godex: unknown fields are tolerated (the venue
// sends many extras), but missing or mistyped required fields and unknown WS
// discriminators are errors — the connection is aborted instead of guessed at.
// Required-ness is enforced with pointer fields plus validate methods.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

func missingField(object, field string) error {
	return fmt.Errorf("dydx: %s is missing required field %q", object, field)
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

// parseUint32Field parses one of the Indexer's numeric-string identifiers.
func parseUint32Field(object, field, value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("dydx: %s has non-numeric %s %q", object, field, value)
	}
	return uint32(parsed), nil
}

// perpetualMarket is the order-placement metadata subset of
// GET /v4/perpetualMarkets.
type perpetualMarket struct {
	Ticker     *string `json:"ticker"`
	ClobPairID *string `json:"clobPairId"`
	Status     *string `json:"status"`
	// Price and size increments as decimal strings (e.g. "1", "0.0001").
	TickSize *string `json:"tickSize"`
	StepSize *string `json:"stepSize"`
	// Integer wire units: base quantums are 10^atomicResolution of the base
	// asset, and an order's size/price must be a multiple of stepBaseQuantums /
	// subticksPerTick respectively.
	AtomicResolution          *int32  `json:"atomicResolution"`
	QuantumConversionExponent *int32  `json:"quantumConversionExponent"`
	StepBaseQuantums          *int64  `json:"stepBaseQuantums"`
	SubticksPerTick           *int64  `json:"subticksPerTick"`
	MaintenanceMarginFraction *string `json:"maintenanceMarginFraction"`
}

func (m *perpetualMarket) validate() error {
	const object = "perpetualMarkets entry"
	if err := checkRequired(object,
		fieldCheck{"ticker", m.Ticker != nil},
		fieldCheck{"clobPairId", m.ClobPairID != nil},
		fieldCheck{"status", m.Status != nil},
		fieldCheck{"tickSize", m.TickSize != nil},
		fieldCheck{"stepSize", m.StepSize != nil},
		fieldCheck{"atomicResolution", m.AtomicResolution != nil},
		fieldCheck{"quantumConversionExponent", m.QuantumConversionExponent != nil},
		fieldCheck{"stepBaseQuantums", m.StepBaseQuantums != nil},
		fieldCheck{"subticksPerTick", m.SubticksPerTick != nil},
		fieldCheck{"maintenanceMarginFraction", m.MaintenanceMarginFraction != nil},
	); err != nil {
		return err
	}
	if *m.StepBaseQuantums <= 0 {
		return fmt.Errorf("dydx: %s has non-positive stepBaseQuantums %d", object, *m.StepBaseQuantums)
	}
	if *m.SubticksPerTick <= 0 {
		return fmt.Errorf("dydx: %s has non-positive subticksPerTick %d", object, *m.SubticksPerTick)
	}
	if _, err := parseUint32Field(object, "clobPairId", *m.ClobPairID); err != nil {
		return err
	}
	return nil
}

// clobPairIDValue returns the market's numeric clob pair id.
func (m *perpetualMarket) clobPairIDValue() (uint32, error) {
	return parseUint32Field("perpetualMarkets entry", "clobPairId", *m.ClobPairID)
}

type perpetualMarketsResponse struct {
	Markets *map[string]perpetualMarket `json:"markets"`
}

func (r *perpetualMarketsResponse) validate() error {
	if r.Markets == nil {
		return missingField("perpetualMarkets", "markets")
	}
	return nil
}

// market returns the entry for ticker, validating only that entry: the venue
// lists every market, and an unrelated one with an unexpected shape must not
// stop the adapter from trading the one it was configured for.
func (r *perpetualMarketsResponse) market(ticker string) (*perpetualMarket, error) {
	entry, ok := (*r.Markets)[ticker]
	if !ok {
		return nil, fmt.Errorf("dydx: market %q is not listed by the venue", ticker)
	}
	if err := entry.validate(); err != nil {
		return nil, err
	}
	if *entry.Status != marketStatusActive {
		return nil, fmt.Errorf("dydx: market %q is %q, not %s", ticker, *entry.Status, marketStatusActive)
	}
	return &entry, nil
}

// perpetualPosition is one open position. entryPrice and unrealizedPnl are
// optional: WebSocket position updates sometimes omit them, which the executor
// treats as a signal to re-read the REST snapshot rather than emit a partial
// position.
type perpetualPosition struct {
	Market        *string `json:"market"`
	Side          *string `json:"side"`
	Size          *string `json:"size"`
	Status        *string `json:"status"`
	EntryPrice    *string `json:"entryPrice"`
	UnrealizedPnl *string `json:"unrealizedPnl"`
}

func (p *perpetualPosition) validate() error {
	const object = "perpetual position"
	if err := checkRequired(object,
		fieldCheck{"market", p.Market != nil},
		fieldCheck{"side", p.Side != nil},
		fieldCheck{"size", p.Size != nil},
	); err != nil {
		return err
	}
	if *p.Side != positionSideLong && *p.Side != positionSideShort {
		return fmt.Errorf("dydx: %s has unknown side %q", object, *p.Side)
	}
	return nil
}

// complete reports whether the position carries the priced fields godex needs.
func (p *perpetualPosition) complete() bool {
	return p.EntryPrice != nil && p.UnrealizedPnl != nil
}

// positionsContainer accepts both shapes the venue uses: the REST/subscribed
// snapshot keys open positions by ticker, while WebSocket updates send an array.
type positionsContainer []perpetualPosition

func (c *positionsContainer) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, (*[]perpetualPosition)(c))
	}
	var byTicker map[string]perpetualPosition
	if err := json.Unmarshal(data, &byTicker); err != nil {
		return err
	}
	*c = nil
	for _, position := range byTicker {
		*c = append(*c, position)
	}
	return nil
}

func isJSONArray(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '['
}

// subaccount is the account-state snapshot: GET /v4/addresses/{address}/
// subaccountNumber/{n} and the WebSocket subscribed payload share this shape.
type subaccount struct {
	Address                *string            `json:"address"`
	SubaccountNumber       *uint32            `json:"subaccountNumber"`
	Equity                 *string            `json:"equity"`
	FreeCollateral         *string            `json:"freeCollateral"`
	OpenPerpetualPositions positionsContainer `json:"openPerpetualPositions"`
}

func (s *subaccount) validate() error {
	const object = "subaccount"
	if err := checkRequired(object,
		fieldCheck{"address", s.Address != nil},
		fieldCheck{"subaccountNumber", s.SubaccountNumber != nil},
		fieldCheck{"equity", s.Equity != nil},
		fieldCheck{"freeCollateral", s.FreeCollateral != nil},
	); err != nil {
		return err
	}
	for i := range s.OpenPerpetualPositions {
		if err := s.OpenPerpetualPositions[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// subaccountResponse is GET /v4/addresses/{address}/subaccountNumber/{n}.
type subaccountResponse struct {
	Subaccount *subaccount `json:"subaccount"`
}

func (r *subaccountResponse) validate() error {
	if r.Subaccount == nil {
		return missingField("subaccount response", "subaccount")
	}
	return r.Subaccount.validate()
}

// orderUpdate is an Indexer order record, from either GET /v4/orders or a
// WebSocket update. Order removals (including post-only crossings and
// short-term expiries) surface here.
type orderUpdate struct {
	// ID is the venue's own order identifier, which fills reference.
	ID            *string `json:"id"`
	ClientID      *string `json:"clientId"`
	ClobPairID    *string `json:"clobPairId"`
	Status        *string `json:"status"`
	Side          *string `json:"side"`
	GoodTilBlock  *string `json:"goodTilBlock"`
	RemovalReason *string `json:"removalReason"`
}

func (o *orderUpdate) validate() error {
	const object = "order update"
	if err := checkRequired(object,
		fieldCheck{"clientId", o.ClientID != nil},
		fieldCheck{"status", o.Status != nil},
	); err != nil {
		return err
	}
	if _, err := strconv.ParseUint(*o.ClientID, 10, 32); err != nil {
		return fmt.Errorf("dydx: %s has non-numeric clientId %q", object, *o.ClientID)
	}
	return nil
}

type ordersResponse []orderUpdate

func (r ordersResponse) validate() error {
	for i := range r {
		if err := r[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// fill is an execution record from GET /v4/fills or a WebSocket update. The
// Indexer's own id is what suppresses duplicates across a reconnect.
type fill struct {
	ID        *string `json:"id"`
	Side      *string `json:"side"`
	Price     *string `json:"price"`
	Size      *string `json:"size"`
	CreatedAt *string `json:"createdAt"`
	// ClientMetadata and OrderID identify the originating order. The Indexer
	// reports its own order id, not the client id, so fills are attributed via
	// the executor's order table.
	OrderID *string `json:"orderId"`
	Market  *string `json:"market"`
}

func (f *fill) validate() error {
	const object = "fill"
	if err := checkRequired(object,
		fieldCheck{"id", f.ID != nil},
		fieldCheck{"side", f.Side != nil},
		fieldCheck{"price", f.Price != nil},
		fieldCheck{"size", f.Size != nil},
		fieldCheck{"createdAt", f.CreatedAt != nil},
		// A subaccount can trade several markets, and FillEvent carries no
		// market of its own — so without this the executor cannot tell its own
		// executions from another market's.
		fieldCheck{"market", f.Market != nil},
	); err != nil {
		return err
	}
	if *f.ID == "" {
		return fmt.Errorf("dydx: %s has an empty id", object)
	}
	if *f.Side != fillSideBuy && *f.Side != fillSideSell {
		return fmt.Errorf("dydx: %s has unknown side %q", object, *f.Side)
	}
	return nil
}

// fillsResponse is GET /v4/fills.
type fillsResponse struct {
	Fills *[]fill `json:"fills"`
}

func (r *fillsResponse) validate() error {
	if r.Fills == nil {
		return missingField("fills response", "fills")
	}
	for i := range *r.Fills {
		if err := (*r.Fills)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// ordersListResponse is GET /v4/orders, a bare JSON array.
type ordersListResponse struct {
	Orders ordersResponse
}

func (r *ordersListResponse) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Orders)
}

func (r *ordersListResponse) validate() error { return r.Orders.validate() }

// subaccountContents is the payload of both the subscribed snapshot and the
// incremental channel_data updates on the v4_subaccounts channel.
type subaccountContents struct {
	Subaccount         *subaccount        `json:"subaccount"`
	PerpetualPositions positionsContainer `json:"perpetualPositions"`
	Orders             ordersResponse     `json:"orders"`
	Fills              []fill             `json:"fills"`
	BlockHeight        *string            `json:"blockHeight"`
}

func (c *subaccountContents) validate() error {
	if c.Subaccount != nil {
		if err := c.Subaccount.validate(); err != nil {
			return err
		}
	}
	for i := range c.PerpetualPositions {
		if err := c.PerpetualPositions[i].validate(); err != nil {
			return err
		}
	}
	if err := c.Orders.validate(); err != nil {
		return err
	}
	for i := range c.Fills {
		if err := c.Fills[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// wsMessage is one decoded frame from the Indexer WebSocket.
type wsMessage struct {
	Type     string
	Channel  string
	Contents *subaccountContents
}

// decodeSubaccountWsMessage parses one frame. Unknown discriminators are
// errors: the caller aborts the connection rather than guessing.
func decodeSubaccountWsMessage(raw []byte) (wsMessage, error) {
	var envelope struct {
		Type    *string `json:"type"`
		Channel *string `json:"channel"`
		Message *string `json:"message"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return wsMessage{}, fmt.Errorf("dydx: malformed ws message: %w", err)
	}
	if envelope.Type == nil {
		return wsMessage{}, missingField("ws message", "type")
	}
	channel := ""
	if envelope.Channel != nil {
		channel = *envelope.Channel
	}

	switch *envelope.Type {
	case wsTypeConnected, wsTypePong, wsTypeUnsubscribed:
		return wsMessage{Type: *envelope.Type, Channel: channel}, nil
	case wsTypeError:
		message := "unspecified"
		if envelope.Message != nil {
			message = *envelope.Message
		}
		return wsMessage{}, fmt.Errorf("dydx: venue ws error: %s", message)
	case wsTypeSubscribed, wsTypeChannelData:
		if channel != subaccountsChannel {
			return wsMessage{}, fmt.Errorf("dydx: %s message for unexpected channel %q",
				*envelope.Type, channel)
		}
		var payload struct {
			Contents *subaccountContents `json:"contents"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return wsMessage{}, fmt.Errorf("dydx: malformed %s payload: %w", *envelope.Type, err)
		}
		if payload.Contents == nil {
			return wsMessage{}, missingField(*envelope.Type+" message", "contents")
		}
		if err := payload.Contents.validate(); err != nil {
			return wsMessage{}, err
		}
		return wsMessage{Type: *envelope.Type, Channel: channel, Contents: payload.Contents}, nil
	default:
		return wsMessage{}, fmt.Errorf("dydx: unknown ws message type %q", *envelope.Type)
	}
}

// cometStatusResponse is CometBFT RPC GET /status.
type cometStatusResponse struct {
	Result *struct {
		SyncInfo *struct {
			LatestBlockHeight *string `json:"latest_block_height"`
			CatchingUp        *bool   `json:"catching_up"`
		} `json:"sync_info"`
	} `json:"result"`
}

func (r *cometStatusResponse) validate() error {
	const object = "status response"
	if r.Result == nil {
		return missingField(object, "result")
	}
	if r.Result.SyncInfo == nil {
		return missingField(object, "result.sync_info")
	}
	if r.Result.SyncInfo.LatestBlockHeight == nil {
		return missingField(object, "result.sync_info.latest_block_height")
	}
	// A syncing node's height is behind the chain tip; deriving good_til_block
	// from it would produce orders the chain has already passed.
	if r.Result.SyncInfo.CatchingUp != nil && *r.Result.SyncInfo.CatchingUp {
		return fmt.Errorf("dydx: rpc node is still catching up; its height is not the chain tip")
	}
	return nil
}

// height returns the node's latest block height.
func (r *cometStatusResponse) height() (uint32, error) {
	parsed, err := strconv.ParseUint(*r.Result.SyncInfo.LatestBlockHeight, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("dydx: status response has non-numeric latest_block_height %q",
			*r.Result.SyncInfo.LatestBlockHeight)
	}
	return uint32(parsed), nil
}

// jsonRPCError is the JSON-RPC envelope-level failure (malformed request,
// method not found), distinct from an ABCI-level rejection.
type jsonRPCError struct {
	Code    *int    `json:"code"`
	Message *string `json:"message"`
	Data    *string `json:"data"`
}

func (e *jsonRPCError) String() string {
	message := "unspecified"
	if e.Message != nil {
		message = *e.Message
	}
	if e.Data != nil && *e.Data != "" {
		message += ": " + *e.Data
	}
	return message
}

// broadcastTxResponse is CometBFT RPC broadcast_tx_sync. A zero Code means the
// transaction entered the mempool — not that it filled.
type broadcastTxResponse struct {
	Result *struct {
		Code      *int    `json:"code"`
		Codespace *string `json:"codespace"`
		Log       *string `json:"log"`
		Hash      *string `json:"hash"`
	} `json:"result"`
	Error *jsonRPCError `json:"error"`
}

func (r *broadcastTxResponse) validate() error {
	if r.Error != nil {
		return fmt.Errorf("dydx: broadcast_tx_sync rpc error: %s", r.Error)
	}
	if r.Result == nil {
		return missingField("broadcast_tx_sync response", "result")
	}
	if r.Result.Code == nil {
		return missingField("broadcast_tx_sync response", "result.code")
	}
	return nil
}

// abciQueryResponse is CometBFT RPC abci_query. Value is base64-encoded
// protobuf.
type abciQueryResponse struct {
	Result *struct {
		Response *struct {
			Code      *int    `json:"code"`
			Codespace *string `json:"codespace"`
			Log       *string `json:"log"`
			Value     *string `json:"value"`
		} `json:"response"`
	} `json:"result"`
	Error *jsonRPCError `json:"error"`
}

func (r *abciQueryResponse) validate() error {
	const object = "abci_query response"
	if r.Error != nil {
		return fmt.Errorf("dydx: abci_query rpc error: %s", r.Error)
	}
	if r.Result == nil {
		return missingField(object, "result")
	}
	if r.Result.Response == nil {
		return missingField(object, "result.response")
	}
	response := r.Result.Response
	if response.Code == nil {
		return missingField(object, "result.response.code")
	}
	if *response.Code != txCodeOK {
		log := ""
		if response.Log != nil {
			log = *response.Log
		}
		return fmt.Errorf("dydx: abci_query failed with code %d: %s", *response.Code, log)
	}
	if response.Value == nil {
		return missingField(object, "result.response.value")
	}
	return nil
}
