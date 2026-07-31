package hyperliquid

// Strict decoding of Hyperliquid REST/WS payloads. Policy: unknown fields are
// tolerated (payloads carry many extras), but missing or mistyped required
// fields and unknown discriminators are errors — the connection is aborted
// instead of guessing (fail fast). Required-ness is enforced with pointer
// fields plus validate methods.

import (
	"encoding/json"
	"fmt"
)

func missingField(object, field string) error {
	return fmt.Errorf("hyperliquid: %s is missing required field %q", object, field)
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

// --- /info: meta ---

// metaAsset is one perp in the universe. Its index in the universe array is
// the asset id every order and cancel is keyed by, so the array order is
// protocol, not presentation.
type metaAsset struct {
	Name        *string `json:"name"`
	SzDecimals  *int    `json:"szDecimals"`
	MaxLeverage *int    `json:"maxLeverage"`
	IsDelisted  *bool   `json:"isDelisted"`
	// OnlyIsolated marks a perp that cannot be held on cross margin.
	OnlyIsolated *bool `json:"onlyIsolated"`
	// MarginTableID names the tier schedule this perp's maintenance margin
	// follows. MaxLeverage is only the first tier's cap, so the table is what
	// says how much margin a large position actually needs.
	MarginTableID *int `json:"marginTableId"`
}

func (a *metaAsset) validate() error {
	const object = "meta universe entry"
	if err := checkRequired(object,
		fieldCheck{"name", a.Name != nil},
		fieldCheck{"szDecimals", a.SzDecimals != nil},
		fieldCheck{"maxLeverage", a.MaxLeverage != nil},
		fieldCheck{"marginTableId", a.MarginTableID != nil},
	); err != nil {
		return err
	}
	if *a.SzDecimals < 0 || *a.SzDecimals > maxPerpPriceDecimals {
		return fmt.Errorf("hyperliquid: %s %q has szDecimals %d outside 0..%d",
			object, *a.Name, *a.SzDecimals, maxPerpPriceDecimals)
	}
	if *a.MaxLeverage <= 0 {
		return fmt.Errorf("hyperliquid: %s %q has non-positive maxLeverage %d", object, *a.Name, *a.MaxLeverage)
	}
	return nil
}

// marginTier is one notional band of a maintenance-margin schedule: above
// LowerBound of position notional, leverage is capped at MaxLeverage.
type marginTier struct {
	LowerBound  *string `json:"lowerBound"`
	MaxLeverage *int    `json:"maxLeverage"`
}

type marginTable struct {
	MarginTiers *[]marginTier `json:"marginTiers"`
}

func (t *marginTable) validate(id int) error {
	if t.MarginTiers == nil {
		return fmt.Errorf("hyperliquid: margin table %d is missing required field %q", id, "marginTiers")
	}
	if len(*t.MarginTiers) == 0 {
		return fmt.Errorf("hyperliquid: margin table %d has no tiers", id)
	}
	for _, tier := range *t.MarginTiers {
		if tier.MaxLeverage == nil {
			return fmt.Errorf("hyperliquid: margin table %d has a tier without %q", id, "maxLeverage")
		}
		if *tier.MaxLeverage <= 0 {
			return fmt.Errorf("hyperliquid: margin table %d has a tier with non-positive maxLeverage %d",
				id, *tier.MaxLeverage)
		}
	}
	return nil
}

// marginTableEntry is one [id, table] pair. The venue sends the pair as a
// heterogeneous array, so it is decoded positionally rather than by field.
type marginTableEntry struct {
	id    int
	table marginTable
}

func (e *marginTableEntry) UnmarshalJSON(data []byte) error {
	var pair []json.RawMessage
	if err := json.Unmarshal(data, &pair); err != nil {
		return fmt.Errorf("hyperliquid: margin table entry is not an array: %w", err)
	}
	if len(pair) != 2 {
		return fmt.Errorf("hyperliquid: margin table entry has %d elements, want 2", len(pair))
	}
	if err := json.Unmarshal(pair[0], &e.id); err != nil {
		return fmt.Errorf("hyperliquid: margin table id is not an integer: %w", err)
	}
	if err := json.Unmarshal(pair[1], &e.table); err != nil {
		return fmt.Errorf("hyperliquid: margin table %d is malformed: %w", e.id, err)
	}
	return nil
}

type metaResponse struct {
	Universe *[]metaAsset `json:"universe"`
	// MarginTables carries only the tiered schedules; the venue omits the
	// flat default tables, so a lookup miss is expected rather than an error.
	MarginTables *[]marginTableEntry `json:"marginTables"`
}

func (r *metaResponse) validate() error {
	if r.Universe == nil {
		return missingField("meta", "universe")
	}
	for i := range *r.Universe {
		if err := (*r.Universe)[i].validate(); err != nil {
			return err
		}
	}
	if r.MarginTables != nil {
		for i := range *r.MarginTables {
			entry := &(*r.MarginTables)[i]
			if err := entry.table.validate(entry.id); err != nil {
				return err
			}
		}
	}
	return nil
}

// marginTablesByID indexes the tiered schedules the response carried.
func (r *metaResponse) marginTablesByID() map[int]*marginTable {
	tables := make(map[int]*marginTable)
	if r.MarginTables == nil {
		return tables
	}
	for i := range *r.MarginTables {
		entry := &(*r.MarginTables)[i]
		tables[entry.id] = &entry.table
	}
	return tables
}

// --- /info: clearinghouseState ---

type leverageWire struct {
	Type  *string `json:"type"`
	Value *int    `json:"value"`
}

func (l *leverageWire) validate() error {
	if l.Type == nil {
		return missingField("position leverage", "type")
	}
	return nil
}

// positionWire is one open perp position. entryPx is absent only when the
// venue has no position to describe; a sized position without a price is
// rejected rather than published (see normalize.go).
type positionWire struct {
	Coin          *string       `json:"coin"`
	Szi           *string       `json:"szi"`
	EntryPx       *string       `json:"entryPx"`
	PositionValue *string       `json:"positionValue"`
	UnrealizedPnl *string       `json:"unrealizedPnl"`
	MarginUsed    *string       `json:"marginUsed"`
	Leverage      *leverageWire `json:"leverage"`
	LiquidationPx *string       `json:"liquidationPx"`
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
	TotalNtlPos     *string `json:"totalNtlPos"`
	TotalRawUsd     *string `json:"totalRawUsd"`
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
	// Time is present on some deployments and absent on others; the adapter
	// stamps its own observation time rather than depending on it.
	Time *int64 `json:"time"`
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

// --- /exchange ---

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
	Oid   *int64  `json:"oid"`
	Cloid *string `json:"cloid"`
}

// filledStatus reports an order that executed on submission.
type filledStatus struct {
	TotalSz *string `json:"totalSz"`
	AvgPx   *string `json:"avgPx"`
	Oid     *int64  `json:"oid"`
	Cloid   *string `json:"cloid"`
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
		return fmt.Errorf("hyperliquid: exchange status entry has %d recognized outcomes, want exactly 1", populated)
	}
	return nil
}

// --- WebSocket ---

type wsEnvelope struct {
	Channel *string         `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

func (e *wsEnvelope) validate() error {
	if e.Channel == nil {
		return missingField("ws message", "channel")
	}
	return nil
}

// wsFill is one execution on the userFills channel — the only source of truth
// for fills.
type wsFill struct {
	Coin  *string `json:"coin"`
	Px    *string `json:"px"`
	Sz    *string `json:"sz"`
	Side  *string `json:"side"`
	Time  *int64  `json:"time"`
	Oid   *int64  `json:"oid"`
	Tid   *int64  `json:"tid"`
	Cloid *string `json:"cloid"`
	Fee   *string `json:"fee"`
}

func (f *wsFill) validate() error {
	const object = "userFills fill"
	if err := checkRequired(object,
		fieldCheck{"coin", f.Coin != nil},
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
		return fmt.Errorf("hyperliquid: %s has unknown side %q", object, *f.Side)
	}
	return nil
}

type wsUserFills struct {
	IsSnapshot *bool     `json:"isSnapshot"`
	User       *string   `json:"user"`
	Fills      *[]wsFill `json:"fills"`
}

func (u *wsUserFills) validate() error {
	if u.Fills == nil {
		return missingField("userFills", "fills")
	}
	for i := range *u.Fills {
		if err := (*u.Fills)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

type wsBasicOrder struct {
	Coin    *string `json:"coin"`
	Side    *string `json:"side"`
	LimitPx *string `json:"limitPx"`
	Sz      *string `json:"sz"`
	Oid     *int64  `json:"oid"`
	OrigSz  *string `json:"origSz"`
	Cloid   *string `json:"cloid"`
}

// wsOrderUpdate is one entry on the orderUpdates channel.
type wsOrderUpdate struct {
	Order           *wsBasicOrder `json:"order"`
	Status          *string       `json:"status"`
	StatusTimestamp *int64        `json:"statusTimestamp"`
}

func (u *wsOrderUpdate) validate() error {
	const object = "orderUpdates entry"
	if err := checkRequired(object,
		fieldCheck{"order", u.Order != nil},
		fieldCheck{"status", u.Status != nil},
	); err != nil {
		return err
	}
	if err := checkRequired(object+" order",
		fieldCheck{"coin", u.Order.Coin != nil},
		fieldCheck{"oid", u.Order.Oid != nil},
	); err != nil {
		return err
	}
	if !isKnownOrderStatus(*u.Status) {
		return fmt.Errorf("hyperliquid: %s has unknown status %q", object, *u.Status)
	}
	return nil
}

func isKnownOrderStatus(status string) bool {
	switch status {
	case orderStatusOpen, orderStatusFilled, orderStatusTriggered:
		return true
	}
	_, closed := orderStatusClosed[status]
	return closed
}

// --- /info: orderStatus ---

// orderQueryResponse answers "does the venue hold this order?", the question
// an ambiguous submission has to settle before trading resumes.
type orderQueryResponse struct {
	Status *string `json:"status"`
	Order  *struct {
		// Status is the order's own lifecycle status, which says whether the
		// venue still holds it live.
		Status *string `json:"status"`
	} `json:"order"`
}

func (r *orderQueryResponse) validate() error {
	if r.Status == nil {
		return missingField("orderStatus", "status")
	}
	if *r.Status != queryStatusOrder && *r.Status != queryStatusUnknownOid {
		return fmt.Errorf("hyperliquid: orderStatus returned unknown status %q", *r.Status)
	}
	// The inner status is optional: an order the venue holds is proof enough
	// for reconciliation even when its lifecycle status is not reported. One
	// that is reported, though, must be a status the adapter understands.
	if r.Order != nil && r.Order.Status != nil && !isKnownOrderStatus(*r.Order.Status) {
		return fmt.Errorf("hyperliquid: orderStatus returned unknown order status %q", *r.Order.Status)
	}
	return nil
}

// --- /info: activeAssetData ---

// activeAssetDataResponse reports the account's per-coin trading settings.
// It is the only place the margin mode is visible for a coin the account
// holds no position in, which clearinghouseState omits entirely.
type activeAssetDataResponse struct {
	Coin     *string       `json:"coin"`
	Leverage *leverageWire `json:"leverage"`
}

func (r *activeAssetDataResponse) validate() error {
	if err := checkRequired("activeAssetData",
		fieldCheck{"coin", r.Coin != nil},
		fieldCheck{"leverage", r.Leverage != nil},
	); err != nil {
		return err
	}
	return r.Leverage.validate()
}

// --- /info: extraAgents ---

type extraAgent struct {
	Address *string `json:"address"`
}

// extraAgentList is the account's approved agent wallets. The venue answers
// with a bare array, and with null when there are none.
type extraAgentList []extraAgent

func (l *extraAgentList) validate() error {
	for _, agent := range *l {
		if agent.Address == nil {
			return missingField("extraAgents entry", "address")
		}
	}
	return nil
}
