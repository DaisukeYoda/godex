package dydx

// Strict decoding of the Indexer's v4_orderbook WebSocket frames and the
// market-statistics subset of GET /v4/perpetualMarkets. Same policy as
// wire.go: unknown fields are tolerated, missing or mistyped required fields
// and unknown discriminators are errors.

import (
	"encoding/json"
	"fmt"

	"github.com/DaisukeYoda/godex/internal/book"
)

// wsBookLevel is the snapshot level representation: {"price": "...", "size": "..."}.
type wsBookLevel struct {
	Price *string `json:"price"`
	Size  *string `json:"size"`
}

func (l *wsBookLevel) toRaw(object string) (book.RawLevel, error) {
	if l.Price == nil || l.Size == nil {
		return book.RawLevel{}, fmt.Errorf("dydx: %s level is missing price or size", object)
	}
	return book.RawLevel{Price: *l.Price, Size: *l.Size}, nil
}

// wsBookSnapshot is the subscribed payload of the v4_orderbook channel.
type wsBookSnapshot struct {
	Bids []wsBookLevel `json:"bids"`
	Asks []wsBookLevel `json:"asks"`
}

func (s *wsBookSnapshot) toRaw() (bids, asks []book.RawLevel, err error) {
	const object = "orderbook snapshot"
	bids = make([]book.RawLevel, 0, len(s.Bids))
	for i := range s.Bids {
		level, err := s.Bids[i].toRaw(object)
		if err != nil {
			return nil, nil, err
		}
		bids = append(bids, level)
	}
	asks = make([]book.RawLevel, 0, len(s.Asks))
	for i := range s.Asks {
		level, err := s.Asks[i].toRaw(object)
		if err != nil {
			return nil, nil, err
		}
		asks = append(asks, level)
	}
	return bids, asks, nil
}

// wsBookDelta is the channel_data payload: levels as [price, size] tuples,
// size "0" meaning level removal.
type wsBookDelta struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
}

func (d *wsBookDelta) validate() error {
	const object = "orderbook delta"
	for _, side := range [][][]string{d.Bids, d.Asks} {
		for _, tuple := range side {
			if len(tuple) != 2 {
				return fmt.Errorf("dydx: %s level must be a [price, size] tuple, got %d elements",
					object, len(tuple))
			}
		}
	}
	return nil
}

// marketWsMessage is one decoded frame from the v4_orderbook stream.
type marketWsMessage struct {
	Type      string
	MessageID int64
	// ID is the market ticker the frame belongs to (subscribed / unsubscribed /
	// channel_data).
	ID       string
	Snapshot *wsBookSnapshot
	Delta    *wsBookDelta
}

// decodeOrderbookWsMessage parses one frame. Unknown discriminators are
// errors: the caller aborts the connection rather than guessing.
func decodeOrderbookWsMessage(raw []byte) (marketWsMessage, error) {
	var envelope struct {
		Type      *string `json:"type"`
		MessageID *int64  `json:"message_id"`
		Channel   *string `json:"channel"`
		ID        *string `json:"id"`
		Message   *string `json:"message"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return marketWsMessage{}, fmt.Errorf("dydx: malformed ws message: %w", err)
	}
	if envelope.Type == nil {
		return marketWsMessage{}, missingField("ws message", "type")
	}
	messageType := *envelope.Type

	if messageType == wsTypeError {
		message := "unspecified"
		if envelope.Message != nil {
			message = *envelope.Message
		}
		return marketWsMessage{}, fmt.Errorf("dydx: venue ws error: %s", message)
	}
	if messageType == wsTypePong {
		return marketWsMessage{Type: messageType}, nil
	}
	if envelope.MessageID == nil {
		return marketWsMessage{}, missingField(messageType+" message", "message_id")
	}
	decoded := marketWsMessage{Type: messageType, MessageID: *envelope.MessageID}

	switch messageType {
	case wsTypeConnected:
		return decoded, nil
	case wsTypeSubscribed, wsTypeUnsubscribed, wsTypeChannelData:
		if envelope.Channel == nil || *envelope.Channel != orderbookChannel {
			channel := ""
			if envelope.Channel != nil {
				channel = *envelope.Channel
			}
			return marketWsMessage{}, fmt.Errorf("dydx: %s message for unexpected channel %q",
				messageType, channel)
		}
		if envelope.ID == nil || *envelope.ID == "" {
			return marketWsMessage{}, missingField(messageType+" message", "id")
		}
		decoded.ID = *envelope.ID
	default:
		return marketWsMessage{}, fmt.Errorf("dydx: unknown ws message type %q", messageType)
	}

	switch messageType {
	case wsTypeSubscribed:
		var payload struct {
			Contents *wsBookSnapshot `json:"contents"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return marketWsMessage{}, fmt.Errorf("dydx: malformed subscribed payload: %w", err)
		}
		if payload.Contents == nil {
			return marketWsMessage{}, missingField("subscribed message", "contents")
		}
		decoded.Snapshot = payload.Contents
	case wsTypeChannelData:
		var payload struct {
			Contents *wsBookDelta `json:"contents"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return marketWsMessage{}, fmt.Errorf("dydx: malformed channel_data payload: %w", err)
		}
		if payload.Contents == nil {
			return marketWsMessage{}, missingField("channel_data message", "contents")
		}
		if err := payload.Contents.validate(); err != nil {
			return marketWsMessage{}, err
		}
		decoded.Delta = payload.Contents
	}
	return decoded, nil
}

// marketStatsMarket is the market-statistics subset of a perpetualMarkets
// entry. Kept separate from perpetualMarket (wire.go): the execution path
// must not start failing because a statistics field changed shape.
type marketStatsMarket struct {
	OraclePrice *string `json:"oraclePrice"`
	// NextFundingRate is the next 1-hour funding rate at unpredictable native
	// precision (e.g. "-0.00000078846153846154").
	NextFundingRate *string `json:"nextFundingRate"`
	// OpenInterest is in base-asset units.
	OpenInterest *string `json:"openInterest"`
	// Volume24H is USD-denominated.
	Volume24H *string `json:"volume24H"`
}

func (m *marketStatsMarket) validate() error {
	return checkRequired("perpetualMarkets stats entry",
		fieldCheck{"oraclePrice", m.OraclePrice != nil},
		fieldCheck{"nextFundingRate", m.NextFundingRate != nil},
		fieldCheck{"openInterest", m.OpenInterest != nil},
		fieldCheck{"volume24H", m.Volume24H != nil},
	)
}

// marketStatsResponse is the statistics view of GET /v4/perpetualMarkets.
type marketStatsResponse struct {
	Markets *map[string]marketStatsMarket `json:"markets"`
}

func (r *marketStatsResponse) validate() error {
	if r.Markets == nil {
		return missingField("perpetualMarkets", "markets")
	}
	return nil
}

// market returns the entry for ticker, validating only that entry.
func (r *marketStatsResponse) market(ticker string) (*marketStatsMarket, error) {
	entry, ok := (*r.Markets)[ticker]
	if !ok {
		return nil, fmt.Errorf("dydx: market %q is not listed by the venue", ticker)
	}
	if err := entry.validate(); err != nil {
		return nil, err
	}
	return &entry, nil
}
