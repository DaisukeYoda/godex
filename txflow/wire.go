package txflow

// Strict decoding of TxFlow REST/WS payloads, and the exchange actions the
// adapter submits. Policy: unknown fields are tolerated (payloads carry many
// extras), but missing or mistyped required fields and unknown discriminators
// are errors — the connection is aborted instead of guessing (fail fast).
// Required-ness is enforced with pointer fields plus validate methods.

import (
	"encoding/json"
	"fmt"
)

func missingField(object, field string) error {
	return fmt.Errorf("txflow: %s is missing required field %q", object, field)
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

// --- /exchange actions ---
//
// An action is submitted as JSON but signed over its MessagePack encoding, so
// the venue re-encodes the parsed action and compares. Field order, key
// spelling, and integer width are therefore part of the protocol: these types
// are declared in wire order and must never be replaced by Go maps.
//
// The venue's own client signs the action *without* its "type" field —
// {grouping, orders} for an order — and adds the type only to the JSON it
// posts. The Type fields below carry msgpack:"-" to reproduce that.
// UNVERIFIED against a live order: if the venue turns out to hash the type
// too, dropping the "-" tags is the whole fix.

// limitOrderWire is the "limit" branch of an order's type field. The adapter
// places no trigger orders, so no other branch exists.
type limitOrderWire struct {
	Tif string `msgpack:"tif" json:"tif"`
}

type orderTypeWire struct {
	Limit limitOrderWire `msgpack:"limit" json:"limit"`
}

// orderWire is one order in an order action. The abbreviated keys are the
// venue's: a asset, b isBuy, p price, s size, r reduceOnly, t type. No client
// order id is sent: the venue's client sends none, and whether the venue
// would accept one is unverified.
type orderWire struct {
	Asset      int           `msgpack:"a" json:"a"`
	IsBuy      bool          `msgpack:"b" json:"b"`
	Price      string        `msgpack:"p" json:"p"`
	Size       string        `msgpack:"s" json:"s"`
	ReduceOnly bool          `msgpack:"r" json:"r"`
	OrderType  orderTypeWire `msgpack:"t" json:"t"`
}

type orderAction struct {
	Type     string      `msgpack:"-" json:"type"`
	Grouping string      `msgpack:"grouping" json:"grouping"`
	Orders   []orderWire `msgpack:"orders" json:"orders"`
}

// cancelWire cancels one order by venue oid.
type cancelWire struct {
	Asset int   `msgpack:"a" json:"a"`
	Oid   int64 `msgpack:"o" json:"o"`
}

type cancelAction struct {
	Type    string       `msgpack:"-" json:"type"`
	Cancels []cancelWire `msgpack:"cancels" json:"cancels"`
}

// --- /info: perpMeta ---

// perpMetaAsset is one perp in the universe. Unlike the Hyperliquid lineage,
// the asset id is carried explicitly (index) rather than implied by array
// position, and quantization is spelled out as decimal steps.
type perpMetaAsset struct {
	Name          *string `json:"name"`
	Index         *int    `json:"index"`
	SzDecimals    *int    `json:"szDecimals"`
	BasePrecision *string `json:"basePrecision"`
	PriceTick     *string `json:"priceTick"`
	MaxLeverage   *int    `json:"maxLeverage"`
	HaltTrading   *bool   `json:"haltTrading"`
	Delisted      *bool   `json:"delisted"`
	// OnlyIsolated marks a perp that cannot be held on cross margin.
	OnlyIsolated *bool `json:"onlyIsolated"`
}

func (a *perpMetaAsset) validate() error {
	const object = "perpMeta universe entry"
	if err := checkRequired(object,
		fieldCheck{"name", a.Name != nil},
		fieldCheck{"index", a.Index != nil},
		fieldCheck{"szDecimals", a.SzDecimals != nil},
		fieldCheck{"basePrecision", a.BasePrecision != nil},
		fieldCheck{"priceTick", a.PriceTick != nil},
		fieldCheck{"maxLeverage", a.MaxLeverage != nil},
	); err != nil {
		return err
	}
	if *a.Index < 0 {
		return fmt.Errorf("txflow: %s %q has negative index %d", object, *a.Name, *a.Index)
	}
	if *a.SzDecimals < 0 {
		return fmt.Errorf("txflow: %s %q has negative szDecimals %d", object, *a.Name, *a.SzDecimals)
	}
	if *a.MaxLeverage <= 0 {
		return fmt.Errorf("txflow: %s %q has non-positive maxLeverage %d", object, *a.Name, *a.MaxLeverage)
	}
	return nil
}

type perpMetaResponse struct {
	Universe *[]perpMetaAsset `json:"universe"`
}

func (r *perpMetaResponse) validate() error {
	if r.Universe == nil {
		return missingField("perpMeta", "universe")
	}
	for i := range *r.Universe {
		if err := (*r.Universe)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// --- /info: clearinghouseState ---

type leverageWire struct {
	Type  *string `json:"type"`
	Value *int    `json:"value"`
}

func (l *leverageWire) validate() error {
	if l.Type == nil {
		return missingField("leverage", "type")
	}
	return nil
}

// positionWire is one open perp position. coin carries the market name
// ("BTC-USDC"). entryPx is absent only when the venue has no position to
// describe; a sized position without a price is rejected rather than
// published (see normalize.go).
type positionWire struct {
	Coin          *string       `json:"coin"`
	Szi           *string       `json:"szi"`
	EntryPx       *string       `json:"entryPx"`
	UnrealizedPnl *string       `json:"unrealizedPnl"`
	Leverage      *leverageWire `json:"leverage"`
}

func (p *positionWire) validate() error {
	const object = "clearinghouseState position"
	if err := checkRequired(object,
		fieldCheck{"coin", p.Coin != nil},
		fieldCheck{"szi", p.Szi != nil},
		fieldCheck{"unrealizedPnl", p.UnrealizedPnl != nil},
		fieldCheck{"leverage", p.Leverage != nil},
	); err != nil {
		return err
	}
	return p.Leverage.validate()
}

type assetPositionWire struct {
	Position *positionWire `json:"position"`
	Type     *string       `json:"type"`
}

func (a *assetPositionWire) validate() error {
	if a.Position == nil {
		return missingField("clearinghouseState assetPosition", "position")
	}
	return a.Position.validate()
}

type marginSummaryWire struct {
	AccountValue    *string `json:"accountValue"`
	TotalMarginUsed *string `json:"totalMarginUsed"`
}

func (m *marginSummaryWire) validate(object string) error {
	return checkRequired(object,
		fieldCheck{"accountValue", m.AccountValue != nil},
		fieldCheck{"totalMarginUsed", m.TotalMarginUsed != nil},
	)
}

type clearinghouseState struct {
	AssetPositions *[]assetPositionWire `json:"assetPositions"`
	MarginSummary  *marginSummaryWire   `json:"marginSummary"`
	// Withdrawable is the account's free collateral; margin usage is
	// measured against it.
	Withdrawable *string `json:"withdrawable"`
}

func (s *clearinghouseState) validate() error {
	const object = "clearinghouseState"
	if err := checkRequired(object,
		fieldCheck{"assetPositions", s.AssetPositions != nil},
		fieldCheck{"marginSummary", s.MarginSummary != nil},
		fieldCheck{"withdrawable", s.Withdrawable != nil},
	); err != nil {
		return err
	}
	if err := s.MarginSummary.validate(object + " marginSummary"); err != nil {
		return err
	}
	for i := range *s.AssetPositions {
		if err := (*s.AssetPositions)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// --- /info: activeAssetData ---

// activeAssetDataResponse reports the account's per-market trading settings.
// It is the only place the margin mode is visible for a market the account
// holds no position in, which clearinghouseState omits entirely.
type activeAssetDataResponse struct {
	Leverage *leverageWire `json:"leverage"`
}

func (r *activeAssetDataResponse) validate() error {
	if r.Leverage == nil {
		return missingField("activeAssetData", "leverage")
	}
	return r.Leverage.validate()
}

// --- /info: openOrders, historicalOrders ---

// openOrderWire is one resting order. Only its identity matters here: the
// query answers "which of the orders I track does the venue still hold?".
type openOrderWire struct {
	Oid *int64 `json:"oid"`
}

type openOrderList []openOrderWire

func (l *openOrderList) validate() error {
	for _, order := range *l {
		if order.Oid == nil {
			return missingField("openOrders entry", "oid")
		}
	}
	return nil
}

// historicalOrderWire is the order half of a historicalOrders entry: its
// identity and lifecycle status. Sequence-independent, so a reconciling
// executor can attribute a terminal status to a tracked oid.
type historicalOrderWire struct {
	Symbol    *string `json:"symbol"`
	Oid       *int64  `json:"oid"`
	Timestamp *int64  `json:"timestamp"`
	Status    *string `json:"status"`
}

type historicalOrderEntry struct {
	Order *historicalOrderWire `json:"order"`
}

func (e *historicalOrderEntry) validate() error {
	const object = "historicalOrders entry"
	if e.Order == nil {
		return missingField(object, "order")
	}
	if err := checkRequired(object+" order",
		fieldCheck{"symbol", e.Order.Symbol != nil},
		fieldCheck{"oid", e.Order.Oid != nil},
		fieldCheck{"timestamp", e.Order.Timestamp != nil},
		fieldCheck{"status", e.Order.Status != nil},
	); err != nil {
		return err
	}
	if !isKnownOrderStatus(*e.Order.Status) {
		return fmt.Errorf("txflow: %s has unknown status %q", object, *e.Order.Status)
	}
	return nil
}

type historicalOrderList []historicalOrderEntry

func (l *historicalOrderList) validate() error {
	for i := range *l {
		if err := (*l)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

func isKnownOrderStatus(status string) bool {
	switch status {
	case orderStatusOpen, orderStatusPartialFilled, orderStatusFilled, orderStatusTriggered:
		return true
	}
	_, closed := orderStatusClosed[status]
	return closed
}

// --- /info: userFills ---

// fillWire is one execution from the userFills query — the only source of
// truth for fills. symbol carries the market name ("BTC-USDC"); coin carries
// only the base asset and is not read.
type fillWire struct {
	Symbol *string `json:"symbol"`
	Px     *string `json:"px"`
	Sz     *string `json:"sz"`
	Side   *string `json:"side"`
	Time   *int64  `json:"time"`
	Oid    *int64  `json:"oid"`
	Tid    *int64  `json:"tid"`
}

func (f *fillWire) validate() error {
	const object = "userFills fill"
	if err := checkRequired(object,
		fieldCheck{"symbol", f.Symbol != nil},
		fieldCheck{"px", f.Px != nil},
		fieldCheck{"sz", f.Sz != nil},
		fieldCheck{"side", f.Side != nil},
		fieldCheck{"time", f.Time != nil},
		fieldCheck{"oid", f.Oid != nil},
		fieldCheck{"tid", f.Tid != nil},
	); err != nil {
		return err
	}
	if *f.Side != sideBid && *f.Side != sideAsk {
		return fmt.Errorf("txflow: %s has unknown side %q", object, *f.Side)
	}
	return nil
}

type fillList []fillWire

func (l *fillList) validate() error {
	for i := range *l {
		if err := (*l)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// --- /exchange responses ---

// exchangeResponse is the envelope every exchange submission returns. On
// failure the response field is a plain string, so it stays raw until the
// status has been read.
type exchangeResponse struct {
	Status   *string         `json:"status"`
	Response json.RawMessage `json:"response"`
}

func (r *exchangeResponse) validate() error {
	if r.Status == nil {
		return missingField("exchange response", "status")
	}
	return nil
}

type exchangeSuccess struct {
	Type *string           `json:"type"`
	Data *exchangeDataWire `json:"data"`
}

func (s *exchangeSuccess) validate() error {
	if s.Type == nil {
		return missingField("exchange response body", "type")
	}
	return nil
}

// exchangeDataWire holds per-order outcomes. A status entry is either a bare
// string (cancels answer "success") or an object, so entries stay raw until
// the response type is known.
type exchangeDataWire struct {
	Statuses *[]json.RawMessage `json:"statuses"`
}

// restingStatus reports an order that joined the book.
type restingStatus struct {
	Oid *int64 `json:"oid"`
}

// filledStatus reports an order that executed on submission.
type filledStatus struct {
	TotalSz *string `json:"totalSz"`
	AvgPx   *string `json:"avgPx"`
	Oid     *int64  `json:"oid"`
}

// orderStatusWire is the object form of a per-order status. Exactly one field
// is populated; an entry that populates none is an unrecognized outcome and
// is rejected rather than read as success.
type orderStatusWire struct {
	Resting *restingStatus `json:"resting"`
	Filled  *filledStatus  `json:"filled"`
	Error   *string        `json:"error"`
	// Success is set on cancel outcomes delivered as objects rather than as
	// the bare "success" string.
	Success *string `json:"success"`
}

func (s *orderStatusWire) validate() error {
	populated := 0
	if s.Resting != nil {
		if s.Resting.Oid == nil {
			return missingField("exchange resting status", "oid")
		}
		populated++
	}
	if s.Filled != nil {
		if err := checkRequired("exchange filled status",
			fieldCheck{"totalSz", s.Filled.TotalSz != nil},
			fieldCheck{"avgPx", s.Filled.AvgPx != nil},
			fieldCheck{"oid", s.Filled.Oid != nil},
		); err != nil {
			return err
		}
		populated++
	}
	if s.Error != nil {
		populated++
	}
	if s.Success != nil {
		populated++
	}
	if populated != 1 {
		return fmt.Errorf("txflow: exchange status entry has %d recognized outcomes, want exactly 1", populated)
	}
	return nil
}

// --- WebSocket ---

// wsEnvelope is any inbound frame. Data frames carry channel+data; the pong
// carries only method.
type wsEnvelope struct {
	Method  *string         `json:"method"`
	Channel *string         `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

func (e *wsEnvelope) validate() error {
	if e.Channel == nil && e.Method == nil {
		return missingField("ws message", "channel")
	}
	return nil
}

type wsBasicOrder struct {
	Oid *int64 `json:"oid"`
}

// wsOrderUpdate is one entry on the orderUpdates channel. Orders are matched
// to the executor's own by oid alone, so nothing else is required of the
// order object.
type wsOrderUpdate struct {
	Order  *wsBasicOrder `json:"order"`
	Status *string       `json:"status"`
}

func (u *wsOrderUpdate) validate() error {
	const object = "orderUpdates entry"
	if err := checkRequired(object,
		fieldCheck{"order", u.Order != nil},
		fieldCheck{"status", u.Status != nil},
	); err != nil {
		return err
	}
	if u.Order.Oid == nil {
		return missingField(object+" order", "oid")
	}
	if !isKnownOrderStatus(*u.Status) {
		return fmt.Errorf("txflow: %s has unknown status %q", object, *u.Status)
	}
	return nil
}
