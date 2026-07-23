package lighter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestDecodeOrderBookDetails(t *testing.T) {
	var response orderBookDetailsResponse
	if err := json.Unmarshal(loadFixture(t, "order_book_details.json"), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	detail := (*response.OrderBookDetails)[0]
	if *detail.MarketID != 2 || *detail.Status != "active" {
		t.Fatalf("unexpected detail: %+v", detail)
	}
	if *detail.SupportedPriceDecimals != 3 || *detail.SupportedSizeDecimals != 3 {
		t.Fatalf("unexpected decimals: %+v", detail)
	}
	if *detail.MinBaseAmount != "0.050" || *detail.MinQuoteAmount != "10" {
		t.Fatalf("unexpected minimums: %+v", detail)
	}
	if *detail.MaintenanceMarginFraction != 240 {
		t.Fatalf("unexpected mmf: %d", *detail.MaintenanceMarginFraction)
	}
}

func TestDecodeOrderBookDetailsRejectsMissingFields(t *testing.T) {
	var response orderBookDetailsResponse
	raw := `{"code":200,"order_book_details":[{"market_id":2,"symbol":"SOL","status":"active"}]}`
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err == nil || !strings.Contains(err.Error(), "missing required field") {
		t.Fatalf("expected missing-field error, got %v", err)
	}
}

func TestDecodeAccountRest(t *testing.T) {
	var response accountRestResponse
	if err := json.Unmarshal(loadFixture(t, "account_rest.json"), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	account := (*response.Accounts)[0]
	if *account.Collateral != "100.000000" || *account.AvailableBalance != "85.000000" {
		t.Fatalf("unexpected account: %+v", account)
	}
	position := (*account.Positions)[0]
	if *position.Sign != -1 || *position.Position != "0.050" {
		t.Fatalf("unexpected position: %+v", position)
	}
}

func TestDecodeAccountRestRejectsInvalidSign(t *testing.T) {
	raw := `{"code":200,"accounts":[{"collateral":"1","available_balance":"1","total_asset_value":"1",
		"positions":[{"market_id":2,"sign":0,"position":"0","avg_entry_price":"0","margin_mode":0}]}]}`
	var response accountRestResponse
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := response.validate(); err == nil || !strings.Contains(err.Error(), "invalid sign") {
		t.Fatalf("expected invalid-sign error, got %v", err)
	}
}

func TestDecodeWsMessageWithMapContainers(t *testing.T) {
	message, err := decodeAccountWsMessage(loadFixture(t, "ws_account_update_with_trade.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Type != wsTypeUpdateAccountAll {
		t.Fatalf("unexpected type %q", message.Type)
	}
	// Map-shaped containers flatten to slices.
	if len(message.Payload.Trades) != 1 || len(message.Payload.Positions) != 0 {
		t.Fatalf("unexpected containers: %+v", message.Payload)
	}
	trade := message.Payload.Trades[0]
	if *trade.BidClientID != 1783182472088 || *trade.BidAccountID != 48 {
		t.Fatalf("unexpected trade: %+v", trade)
	}
}

func TestDecodeWsMessageControlTypes(t *testing.T) {
	for _, raw := range []string{`{"type":"connected"}`, `{"type":"ping"}`, `{"type":"pong"}`} {
		message, err := decodeAccountWsMessage([]byte(raw))
		if err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if message.Payload != nil {
			t.Fatalf("control message must carry no payload: %s", raw)
		}
	}
}

func TestDecodeWsMessageRejectsUnknownType(t *testing.T) {
	// Unknown discriminators abort the connection (fail fast).
	_, err := decodeAccountWsMessage([]byte(`{"type":"update/pool_data","channel":"pool:1"}`))
	if err == nil || !strings.Contains(err.Error(), "unknown account ws message type") {
		t.Fatalf("expected unknown-type error, got %v", err)
	}
	_, err = decodeAccountWsMessage([]byte(`{"channel":"account_all:48"}`))
	if err == nil || !strings.Contains(err.Error(), "type") {
		t.Fatalf("expected missing-type error, got %v", err)
	}
	_, err = decodeAccountWsMessage([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestDecodeWsMessageRejectsPayloadWithoutChannel(t *testing.T) {
	_, err := decodeAccountWsMessage([]byte(`{"type":"update/account_all"}`))
	if err == nil || !strings.Contains(err.Error(), "channel") {
		t.Fatalf("expected missing-channel error, got %v", err)
	}
}

func TestDecodeSendTxResponses(t *testing.T) {
	tests := []struct {
		raw         string
		wantCode    int
		wantMessage string
	}{
		{`{"code":200,"tx_hash":"0x2f2b"}`, 200, ""},
		{`{"code":21120,"message":"post only order would have matched immediately"}`, 21120, "post only order would have matched immediately"},
		{`{"code":21505,"message":"invalid nonce"}`, 21505, "invalid nonce"},
	}
	for _, tt := range tests {
		var response sendTxResponse
		if err := json.Unmarshal([]byte(tt.raw), &response); err != nil {
			t.Fatalf("unmarshal %s: %v", tt.raw, err)
		}
		if err := response.validate(); err != nil {
			t.Fatalf("validate %s: %v", tt.raw, err)
		}
		if *response.Code != tt.wantCode {
			t.Fatalf("code = %d, want %d", *response.Code, tt.wantCode)
		}
		if tt.wantMessage != "" && (response.Message == nil || *response.Message != tt.wantMessage) {
			t.Fatalf("message = %v, want %q", response.Message, tt.wantMessage)
		}
	}
}
