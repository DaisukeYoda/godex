package dydx

import (
	"strings"
	"testing"
)

func TestDecodeOrderbookWsMessage(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string
		check   func(t *testing.T, decoded marketWsMessage)
	}{
		{
			name: "connected",
			raw:  `{"type":"connected","message_id":0}`,
			check: func(t *testing.T, decoded marketWsMessage) {
				if decoded.Type != wsTypeConnected || decoded.MessageID != 0 {
					t.Fatalf("decoded = %+v", decoded)
				}
			},
		},
		{
			name: "pong is tolerated without a message_id",
			raw:  `{"type":"pong"}`,
			check: func(t *testing.T, decoded marketWsMessage) {
				if decoded.Type != wsTypePong {
					t.Fatalf("decoded = %+v", decoded)
				}
			},
		},
		{
			name: "subscribed snapshot",
			raw: `{"type":"subscribed","message_id":1,"channel":"v4_orderbook","id":"SOL-USD",` +
				`"contents":{"bids":[{"price":"128.41","size":"10"}],"asks":[]}}`,
			check: func(t *testing.T, decoded marketWsMessage) {
				if decoded.ID != "SOL-USD" || decoded.Snapshot == nil {
					t.Fatalf("decoded = %+v", decoded)
				}
				bids, _, err := decoded.Snapshot.toRaw()
				if err != nil {
					t.Fatalf("toRaw: %v", err)
				}
				if len(bids) != 1 || bids[0].Price != "128.41" || bids[0].Size != "10" {
					t.Fatalf("bids = %+v", bids)
				}
			},
		},
		{
			name: "channel_data delta with tuples",
			raw: `{"type":"channel_data","message_id":2,"channel":"v4_orderbook","id":"SOL-USD",` +
				`"contents":{"bids":[["128.41","0"]]}}`,
			check: func(t *testing.T, decoded marketWsMessage) {
				if decoded.Delta == nil || len(decoded.Delta.Bids) != 1 {
					t.Fatalf("decoded = %+v", decoded)
				}
			},
		},
		{
			name:    "malformed json",
			raw:     `{`,
			wantErr: "malformed ws message",
		},
		{
			name:    "missing type",
			raw:     `{"message_id":1}`,
			wantErr: `missing required field "type"`,
		},
		{
			name:    "unknown type",
			raw:     `{"type":"surprise","message_id":1}`,
			wantErr: `unknown ws message type "surprise"`,
		},
		{
			name:    "venue error frame",
			raw:     `{"type":"error","message":"Too many subscribe attempts"}`,
			wantErr: "Too many subscribe attempts",
		},
		{
			name:    "missing message_id",
			raw:     `{"type":"channel_data","channel":"v4_orderbook","id":"SOL-USD","contents":{}}`,
			wantErr: `missing required field "message_id"`,
		},
		{
			name:    "wrong channel",
			raw:     `{"type":"channel_data","message_id":2,"channel":"v4_trades","id":"SOL-USD","contents":{}}`,
			wantErr: `unexpected channel "v4_trades"`,
		},
		{
			name:    "missing id",
			raw:     `{"type":"subscribed","message_id":1,"channel":"v4_orderbook","contents":{"bids":[],"asks":[]}}`,
			wantErr: `missing required field "id"`,
		},
		{
			name:    "missing contents",
			raw:     `{"type":"subscribed","message_id":1,"channel":"v4_orderbook","id":"SOL-USD"}`,
			wantErr: `missing required field "contents"`,
		},
		{
			name: "delta tuple with wrong arity",
			raw: `{"type":"channel_data","message_id":2,"channel":"v4_orderbook","id":"SOL-USD",` +
				`"contents":{"asks":[["128.41"]]}}`,
			wantErr: "must be a [price, size] tuple",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := decodeOrderbookWsMessage([]byte(tc.raw))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			tc.check(t, decoded)
		})
	}
}

func TestSnapshotLevelMissingFields(t *testing.T) {
	raw := `{"type":"subscribed","message_id":1,"channel":"v4_orderbook","id":"SOL-USD",` +
		`"contents":{"bids":[{"price":"128.41"}],"asks":[]}}`
	decoded, err := decodeOrderbookWsMessage([]byte(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Level completeness is enforced at conversion (the boundary the book
	// consumes), not at decode.
	if _, _, err := decoded.Snapshot.toRaw(); err == nil ||
		!strings.Contains(err.Error(), "missing price or size") {
		t.Fatalf("toRaw err = %v, want missing price or size", err)
	}
}

func TestMarketStatsResponseValidation(t *testing.T) {
	response := &marketStatsResponse{}
	if err := response.validate(); err == nil || !strings.Contains(err.Error(), `"markets"`) {
		t.Fatalf("validate err = %v, want missing markets", err)
	}

	markets := map[string]marketStatsMarket{
		"SOL-USD": {}, // all stat fields missing
	}
	response = &marketStatsResponse{Markets: &markets}
	if err := response.validate(); err != nil {
		t.Fatalf("response-level validate must not inspect entries: %v", err)
	}
	if _, err := response.market("SOL-USD"); err == nil ||
		!strings.Contains(err.Error(), "missing required field") {
		t.Fatalf("market err = %v, want missing field", err)
	}
	if _, err := response.market("ETH-USD"); err == nil ||
		!strings.Contains(err.Error(), "not listed") {
		t.Fatalf("market err = %v, want not listed", err)
	}
}
