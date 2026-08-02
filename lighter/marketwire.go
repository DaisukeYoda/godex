package lighter

// Strict decoding of the public order_book WebSocket frames and the funding /
// orderBookDetails REST endpoints. Same policy as wire.go: unknown fields are
// tolerated, missing or mistyped required fields and unknown discriminators
// are errors.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/DaisukeYoda/godex/internal/book"
)

// Market WS protocol.
const (
	// orderbookChannel is the public book channel. Subscriptions are sent as
	// "order_book/{market_id}"; inbound frames carry "order_book:{market_id}"
	// (observed against the live API).
	orderbookChannel = "order_book"

	wsTypeSubscribedOrderBook = "subscribed/order_book"
	wsTypeUpdateOrderBook     = "update/order_book"
)

// wsMarketLevel is a book level: price and size as decimal strings. Size is
// absolute; "0.00000" means the level is absent.
type wsMarketLevel struct {
	Price *string `json:"price"`
	Size  *string `json:"size"`
}

func (l *wsMarketLevel) toRaw(object string) (book.RawLevel, error) {
	if l.Price == nil || l.Size == nil {
		return book.RawLevel{}, fmt.Errorf("lighter: %s level is missing price or size", object)
	}
	return book.RawLevel{Price: *l.Price, Size: *l.Size}, nil
}

// wsOrderBookPayload is the order_book payload shared by snapshot and update
// frames. The matching engine numbers nonce; an update's begin_nonce must
// equal the previous frame's nonce (official WS reference), which is the
// stream's continuity proof.
type wsOrderBookPayload struct {
	Bids       []wsMarketLevel `json:"bids"`
	Asks       []wsMarketLevel `json:"asks"`
	Nonce      *int64          `json:"nonce"`
	BeginNonce *int64          `json:"begin_nonce"`
}

func (p *wsOrderBookPayload) validate(object string) error {
	return checkRequired(object,
		fieldCheck{"nonce", p.Nonce != nil},
		fieldCheck{"begin_nonce", p.BeginNonce != nil},
	)
}

func (p *wsOrderBookPayload) toRaw(object string) (bids, asks []book.RawLevel, err error) {
	bids = make([]book.RawLevel, 0, len(p.Bids))
	for i := range p.Bids {
		level, err := p.Bids[i].toRaw(object)
		if err != nil {
			return nil, nil, err
		}
		bids = append(bids, level)
	}
	asks = make([]book.RawLevel, 0, len(p.Asks))
	for i := range p.Asks {
		level, err := p.Asks[i].toRaw(object)
		if err != nil {
			return nil, nil, err
		}
		asks = append(asks, level)
	}
	return bids, asks, nil
}

// marketWsMessage is one decoded frame from the order_book stream.
type marketWsMessage struct {
	Type string
	// MarketID is parsed from the frame's channel ("order_book:{market_id}").
	MarketID string
	Book     *wsOrderBookPayload
}

// decodeMarketWsMessage parses one frame. Unknown discriminators are errors:
// the caller aborts the connection rather than guessing.
func decodeMarketWsMessage(raw []byte) (marketWsMessage, error) {
	var envelope struct {
		Type    *string `json:"type"`
		Channel *string `json:"channel"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return marketWsMessage{}, fmt.Errorf("lighter: malformed ws message: %w", err)
	}
	if envelope.Type == nil {
		return marketWsMessage{}, missingField("ws message", "type")
	}
	messageType := *envelope.Type

	switch messageType {
	case wsTypeConnected, wsTypePong, wsTypePing:
		return marketWsMessage{Type: messageType}, nil
	case wsTypeSubscribedOrderBook, wsTypeUpdateOrderBook:
		if envelope.Channel == nil {
			return marketWsMessage{}, missingField(messageType+" message", "channel")
		}
		marketID, err := marketIDFromChannel(*envelope.Channel)
		if err != nil {
			return marketWsMessage{}, err
		}
		var payload struct {
			OrderBook *wsOrderBookPayload `json:"order_book"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return marketWsMessage{}, fmt.Errorf("lighter: malformed %s payload: %w", messageType, err)
		}
		if payload.OrderBook == nil {
			return marketWsMessage{}, missingField(messageType+" message", "order_book")
		}
		if err := payload.OrderBook.validate(messageType + " order_book"); err != nil {
			return marketWsMessage{}, err
		}
		return marketWsMessage{Type: messageType, MarketID: marketID, Book: payload.OrderBook}, nil
	default:
		return marketWsMessage{}, fmt.Errorf("lighter: unknown ws message type %q", messageType)
	}
}

// marketIDFromChannel extracts the market id from an inbound channel label
// ("order_book:{market_id}").
func marketIDFromChannel(channel string) (string, error) {
	prefix, marketID, found := strings.Cut(channel, ":")
	if !found || prefix != orderbookChannel || marketID == "" {
		return "", fmt.Errorf("lighter: unexpected channel %q", channel)
	}
	return marketID, nil
}

// fundingEntry is one row of GET /api/v1/fundings. rate is a percent-per-hour
// decimal string (e.g. "0.0008"); its sign lives in direction.
type fundingEntry struct {
	Timestamp *int64  `json:"timestamp"`
	Rate      *string `json:"rate"`
	Direction *string `json:"direction"`
}

func (e *fundingEntry) validate() error {
	const object = "fundings entry"
	if err := checkRequired(object,
		fieldCheck{"timestamp", e.Timestamp != nil},
		fieldCheck{"rate", e.Rate != nil},
		fieldCheck{"direction", e.Direction != nil},
	); err != nil {
		return err
	}
	if *e.Direction != fundingDirectionLong && *e.Direction != fundingDirectionShort {
		return fmt.Errorf("lighter: %s has unknown direction %q", object, *e.Direction)
	}
	return nil
}

// fundingsResponse is GET /api/v1/fundings.
type fundingsResponse struct {
	Code     *int           `json:"code"`
	Fundings []fundingEntry `json:"fundings"`
}

func (r *fundingsResponse) validate() error {
	if r.Code == nil {
		return missingField("fundings response", "code")
	}
	if *r.Code != restSuccessCode {
		return fmt.Errorf("lighter: fundings response code %d", *r.Code)
	}
	for i := range r.Fundings {
		if err := r.Fundings[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// statsBookDetail is one row of GET /api/v1/orderBookDetails. Uniquely for
// this venue's API, the numbers arrive as JSON floats; they feed statistics
// only, never books or orders.
type statsBookDetail struct {
	MarketID *int64 `json:"market_id"`
	// LastTradePrice is the venue's reference price for OI conversion.
	LastTradePrice *float64 `json:"last_trade_price"`
	// DailyQuoteTokenVolume is USDC-denominated 24h volume.
	DailyQuoteTokenVolume *float64 `json:"daily_quote_token_volume"`
	// OpenInterest is in base-asset units.
	OpenInterest *float64 `json:"open_interest"`
}

func (d *statsBookDetail) validate() error {
	return checkRequired("orderBookDetails entry",
		fieldCheck{"market_id", d.MarketID != nil},
		fieldCheck{"last_trade_price", d.LastTradePrice != nil},
		fieldCheck{"daily_quote_token_volume", d.DailyQuoteTokenVolume != nil},
		fieldCheck{"open_interest", d.OpenInterest != nil},
	)
}

// statsBookDetailsResponse is GET /api/v1/orderBookDetails.
type statsBookDetailsResponse struct {
	Code             *int              `json:"code"`
	OrderBookDetails []statsBookDetail `json:"order_book_details"`
}

func (r *statsBookDetailsResponse) validate() error {
	if r.Code == nil {
		return missingField("orderBookDetails response", "code")
	}
	if *r.Code != restSuccessCode {
		return fmt.Errorf("lighter: orderBookDetails response code %d", *r.Code)
	}
	return nil
}

// detail returns the entry for marketID, validating only that entry.
func (r *statsBookDetailsResponse) detail(marketID int64) (*statsBookDetail, error) {
	for i := range r.OrderBookDetails {
		entry := &r.OrderBookDetails[i]
		if entry.MarketID != nil && *entry.MarketID == marketID {
			if err := entry.validate(); err != nil {
				return nil, err
			}
			return entry, nil
		}
	}
	return nil, fmt.Errorf("lighter: market %d not found in orderBookDetails", marketID)
}
