package lighter

import (
	"encoding/json"
	"strings"
	"testing"
)

// Frames mirror the reference implementation's recorded fixtures (omnibook
// lighter-connector, 2026-06-12): BTC is market 1 with price scale 1 and size
// scale 5.

func TestDecodeMarketWsMessage(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantType   string
		wantMarket string
		wantErr    string
	}{
		{
			name:     "connected control message",
			raw:      `{"type":"connected","session_id":"test-session"}`,
			wantType: wsTypeConnected,
		},
		{
			name:     "pong control message",
			raw:      `{"type":"pong"}`,
			wantType: wsTypePong,
		},
		{
			name:     "server-initiated ping",
			raw:      `{"type":"ping"}`,
			wantType: wsTypePing,
		},
		{
			name: "subscribed snapshot",
			raw: `{"type":"subscribed/order_book","channel":"order_book:1","order_book":{` +
				`"bids":[{"price":"63135.0","size":"0.03290"}],"asks":[{"price":"63136.0","size":"0.47240"}],` +
				`"nonce":100,"begin_nonce":0}}`,
			wantType:   wsTypeSubscribedOrderBook,
			wantMarket: "1",
		},
		{
			name: "update",
			raw: `{"type":"update/order_book","channel":"order_book:7","order_book":{` +
				`"bids":[],"asks":[],"nonce":101,"begin_nonce":100}}`,
			wantType:   wsTypeUpdateOrderBook,
			wantMarket: "7",
		},
		{
			name:    "malformed json",
			raw:     `not json`,
			wantErr: "malformed ws message",
		},
		{
			name:    "missing type",
			raw:     `{"channel":"order_book:1"}`,
			wantErr: `missing required field "type"`,
		},
		{
			name:    "unknown type",
			raw:     `{"type":"update/pool_data","channel":"pool:1"}`,
			wantErr: `unknown ws message type "update/pool_data"`,
		},
		{
			name:    "subscribed without channel",
			raw:     `{"type":"subscribed/order_book","order_book":{"bids":[],"asks":[],"nonce":1,"begin_nonce":0}}`,
			wantErr: `missing required field "channel"`,
		},
		{
			name:    "channel without order_book prefix",
			raw:     `{"type":"update/order_book","channel":"trades:1","order_book":{"bids":[],"asks":[],"nonce":1,"begin_nonce":0}}`,
			wantErr: `unexpected channel "trades:1"`,
		},
		{
			name:    "channel in subscribe form instead of inbound form",
			raw:     `{"type":"update/order_book","channel":"order_book/1","order_book":{"bids":[],"asks":[],"nonce":1,"begin_nonce":0}}`,
			wantErr: `unexpected channel "order_book/1"`,
		},
		{
			name:    "channel with empty market id",
			raw:     `{"type":"update/order_book","channel":"order_book:","order_book":{"bids":[],"asks":[],"nonce":1,"begin_nonce":0}}`,
			wantErr: `unexpected channel "order_book:"`,
		},
		{
			name:    "missing order_book payload",
			raw:     `{"type":"update/order_book","channel":"order_book:1"}`,
			wantErr: `missing required field "order_book"`,
		},
		{
			name:    "mistyped order_book payload",
			raw:     `{"type":"update/order_book","channel":"order_book:1","order_book":5}`,
			wantErr: "malformed update/order_book payload",
		},
		{
			name:    "missing nonce",
			raw:     `{"type":"update/order_book","channel":"order_book:1","order_book":{"bids":[],"asks":[],"begin_nonce":0}}`,
			wantErr: `missing required field "nonce"`,
		},
		{
			name:    "missing begin_nonce",
			raw:     `{"type":"subscribed/order_book","channel":"order_book:1","order_book":{"bids":[],"asks":[],"nonce":100}}`,
			wantErr: `missing required field "begin_nonce"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err := decodeMarketWsMessage([]byte(tt.raw))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if message.Type != tt.wantType {
				t.Fatalf("type = %q, want %q", message.Type, tt.wantType)
			}
			if message.MarketID != tt.wantMarket {
				t.Fatalf("market id = %q, want %q", message.MarketID, tt.wantMarket)
			}
			if tt.wantMarket == "" && message.Book != nil {
				t.Fatalf("control message must carry no book: %+v", message.Book)
			}
			if tt.wantMarket != "" && message.Book == nil {
				t.Fatal("book message must carry an order_book payload")
			}
		})
	}
}

func TestDecodeMarketWsMessageSnapshotPayload(t *testing.T) {
	message, err := decodeMarketWsMessage([]byte(
		`{"type":"subscribed/order_book","channel":"order_book:1","order_book":{` +
			`"bids":[{"price":"63133.0","size":"0.03260"},{"price":"63135.0","size":"0.03290"}],` +
			`"asks":[{"price":"63136.0","size":"0.47240"}],"nonce":100,"begin_nonce":0}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if *message.Book.Nonce != 100 || *message.Book.BeginNonce != 0 {
		t.Fatalf("unexpected nonces: %+v", message.Book)
	}
	bids, asks, err := message.Book.toRaw("snapshot")
	if err != nil {
		t.Fatalf("toRaw: %v", err)
	}
	if len(bids) != 2 || len(asks) != 1 {
		t.Fatalf("unexpected level counts: %d bids, %d asks", len(bids), len(asks))
	}
	// Wire order and decimal strings pass through untouched; sorting and
	// parsing are the book builder's job.
	if bids[0].Price != "63133.0" || bids[0].Size != "0.03260" {
		t.Fatalf("unexpected first bid: %+v", bids[0])
	}
	if asks[0].Price != "63136.0" || asks[0].Size != "0.47240" {
		t.Fatalf("unexpected first ask: %+v", asks[0])
	}
}

func TestMarketLevelToRawRejectsMissingPriceOrSize(t *testing.T) {
	for _, raw := range []string{
		`{"bids":[{"size":"0.03260"}],"asks":[],"nonce":1,"begin_nonce":0}`,
		`{"bids":[],"asks":[{"price":"63136.0"}],"nonce":1,"begin_nonce":0}`,
	} {
		var payload wsOrderBookPayload
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		if _, _, err := payload.toRaw("update"); err == nil || !strings.Contains(err.Error(), "missing price or size") {
			t.Fatalf("toRaw(%s) error = %v, want missing price/size", raw, err)
		}
	}
}

func TestDecodeFundingsResponse(t *testing.T) {
	var response fundingsResponse
	if err := json.Unmarshal(loadFixture(t, "market_fundings.json"), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(response.Fundings) != 2 {
		t.Fatalf("entries = %d, want 2", len(response.Fundings))
	}
	latest := response.Fundings[1]
	if *latest.Timestamp != 1781193600 || *latest.Rate != "0.0008" || *latest.Direction != fundingDirectionLong {
		t.Fatalf("unexpected entry: %+v", latest)
	}
}

func TestFundingsResponseValidation(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "short direction is accepted",
			raw:  `{"code":200,"fundings":[{"timestamp":1781193600,"rate":"0.0011","direction":"short"}]}`,
		},
		{
			name: "empty entries pass wire validation",
			raw:  `{"code":200,"fundings":[]}`,
		},
		{
			name:    "missing code",
			raw:     `{"fundings":[]}`,
			wantErr: `missing required field "code"`,
		},
		{
			name:    "business error code",
			raw:     `{"code":20001,"message":"invalid param"}`,
			wantErr: "fundings response code 20001",
		},
		{
			name:    "unknown direction",
			raw:     `{"code":200,"fundings":[{"timestamp":1781193600,"rate":"0.0008","direction":"sideways"}]}`,
			wantErr: `unknown direction "sideways"`,
		},
		{
			name:    "missing timestamp",
			raw:     `{"code":200,"fundings":[{"rate":"0.0008","direction":"long"}]}`,
			wantErr: `missing required field "timestamp"`,
		},
		{
			name:    "missing rate",
			raw:     `{"code":200,"fundings":[{"timestamp":1781193600,"direction":"long"}]}`,
			wantErr: `missing required field "rate"`,
		},
		{
			name:    "missing direction",
			raw:     `{"code":200,"fundings":[{"timestamp":1781193600,"rate":"0.0008"}]}`,
			wantErr: `missing required field "direction"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var response fundingsResponse
			if err := json.Unmarshal([]byte(tt.raw), &response); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			err := response.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeStatsBookDetails(t *testing.T) {
	var response statsBookDetailsResponse
	if err := json.Unmarshal(loadFixture(t, "market_order_book_details.json"), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	detail, err := response.detail(1)
	if err != nil {
		t.Fatalf("detail: %v", err)
	}
	if *detail.LastTradePrice != 62000.2 || *detail.OpenInterest != 1700.5 {
		t.Fatalf("unexpected detail: %+v", detail)
	}
	if *detail.DailyQuoteTokenVolume != 719934279.906264 {
		t.Fatalf("unexpected volume: %v", *detail.DailyQuoteTokenVolume)
	}
	if _, err := response.detail(99); err == nil || !strings.Contains(err.Error(), "market 99 not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

// detail validates only the requested entry: a broken sibling row (market 45
// missing open_interest) must not fail lookups of intact markets.
func TestStatsBookDetailValidatesOnlyRequestedEntry(t *testing.T) {
	raw := `{"code":200,"order_book_details":[` +
		`{"market_id":45,"last_trade_price":0.0123,"daily_quote_token_volume":123456.789},` +
		`{"market_id":1,"last_trade_price":62000.2,"daily_quote_token_volume":719934279.906264,"open_interest":1700.5}]}`
	var response statsBookDetailsResponse
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := response.detail(1); err != nil {
		t.Fatalf("detail(1): %v", err)
	}
	if _, err := response.detail(45); err == nil || !strings.Contains(err.Error(), `missing required field "open_interest"`) {
		t.Fatalf("detail(45) error = %v, want missing open_interest", err)
	}
}

func TestStatsBookDetailsResponseValidation(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "missing code",
			raw:     `{"order_book_details":[]}`,
			wantErr: `missing required field "code"`,
		},
		{
			name:    "business error code",
			raw:     `{"code":20001,"message":"invalid param"}`,
			wantErr: "orderBookDetails response code 20001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var response statsBookDetailsResponse
			if err := json.Unmarshal([]byte(tt.raw), &response); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := response.validate(); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
