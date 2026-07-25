package dydx

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

func decodeFixture[T any, PT interface {
	*T
	validate() error
}](t *testing.T, name string) PT {
	t.Helper()
	var value T
	if err := json.Unmarshal(loadFixture(t, name), PT(&value)); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if err := PT(&value).validate(); err != nil {
		t.Fatalf("validate %s: %v", name, err)
	}
	return &value
}

func TestDecodePerpetualMarkets(t *testing.T) {
	response := decodeFixture[perpetualMarketsResponse](t, "perpetual_markets.json")

	market, err := response.market("ETH-USD")
	if err != nil {
		t.Fatalf("market: %v", err)
	}
	clobPairID, err := market.clobPairIDValue()
	if err != nil {
		t.Fatalf("clobPairIDValue: %v", err)
	}
	if clobPairID != 1 {
		t.Fatalf("clobPairId = %d, want 1", clobPairID)
	}
	if *market.TickSize != "0.1" || *market.StepSize != "0.001" {
		t.Fatalf("tick/step = %s/%s, want 0.1/0.001", *market.TickSize, *market.StepSize)
	}
	if *market.AtomicResolution != -9 || *market.QuantumConversionExponent != -9 {
		t.Fatalf("resolution/exponent = %d/%d, want -9/-9",
			*market.AtomicResolution, *market.QuantumConversionExponent)
	}
	if *market.MaintenanceMarginFraction != "0.03" {
		t.Fatalf("maintenanceMarginFraction = %s, want 0.03", *market.MaintenanceMarginFraction)
	}
}

func TestPerpetualMarketsRejectsUnlistedAndInactiveMarkets(t *testing.T) {
	response := decodeFixture[perpetualMarketsResponse](t, "perpetual_markets.json")

	if _, err := response.market("DOGE-USD"); err == nil {
		t.Fatal("expected an error for a market the venue does not list")
	}
	// A non-ACTIVE market would accept no orders; refuse it at Connect.
	if _, err := response.market("PAUSED-USD"); err == nil {
		t.Fatal("expected an error for a non-ACTIVE market")
	}
}

func TestPerpetualMarketRejectsMissingAndInvalidFields(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "missing tickSize", body: `{"markets":{"ETH-USD":{
			"ticker":"ETH-USD","clobPairId":"1","status":"ACTIVE","stepSize":"0.001",
			"atomicResolution":-9,"quantumConversionExponent":-9,
			"stepBaseQuantums":1000000,"subticksPerTick":100000,
			"maintenanceMarginFraction":"0.03"}}}`},
		{name: "missing atomicResolution", body: `{"markets":{"ETH-USD":{
			"ticker":"ETH-USD","clobPairId":"1","status":"ACTIVE","tickSize":"0.1","stepSize":"0.001",
			"quantumConversionExponent":-9,"stepBaseQuantums":1000000,"subticksPerTick":100000,
			"maintenanceMarginFraction":"0.03"}}}`},
		{name: "zero stepBaseQuantums", body: `{"markets":{"ETH-USD":{
			"ticker":"ETH-USD","clobPairId":"1","status":"ACTIVE","tickSize":"0.1","stepSize":"0.001",
			"atomicResolution":-9,"quantumConversionExponent":-9,
			"stepBaseQuantums":0,"subticksPerTick":100000,
			"maintenanceMarginFraction":"0.03"}}}`},
		{name: "non-numeric clobPairId", body: `{"markets":{"ETH-USD":{
			"ticker":"ETH-USD","clobPairId":"eth","status":"ACTIVE","tickSize":"0.1","stepSize":"0.001",
			"atomicResolution":-9,"quantumConversionExponent":-9,
			"stepBaseQuantums":1000000,"subticksPerTick":100000,
			"maintenanceMarginFraction":"0.03"}}}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var response perpetualMarketsResponse
			if err := json.Unmarshal([]byte(testCase.body), &response); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if _, err := response.market("ETH-USD"); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDecodeSubaccountREST(t *testing.T) {
	response := decodeFixture[subaccountResponse](t, "subaccount_rest.json")

	if *response.Subaccount.Equity != "10000.500000" {
		t.Fatalf("equity = %s", *response.Subaccount.Equity)
	}
	positions := response.Subaccount.OpenPerpetualPositions
	if len(positions) != 1 {
		t.Fatalf("got %d positions, want 1", len(positions))
	}
	// The REST snapshot keys positions by ticker; the container flattens it.
	if *positions[0].Market != "ETH-USD" || *positions[0].Side != positionSideLong {
		t.Fatalf("position = %s %s", *positions[0].Market, *positions[0].Side)
	}
	if !positions[0].complete() {
		t.Fatal("REST snapshot positions carry entryPrice and unrealizedPnl")
	}
}

func TestDecodeWsSubscribed(t *testing.T) {
	message, err := decodeSubaccountWsMessage(loadFixture(t, "ws_subscribed.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Type != wsTypeSubscribed {
		t.Fatalf("type = %q, want %q", message.Type, wsTypeSubscribed)
	}
	if message.Contents.Subaccount == nil {
		t.Fatal("subscribed payload must carry the subaccount snapshot")
	}
	if *message.Contents.Subaccount.FreeCollateral != "9200.250000" {
		t.Fatalf("freeCollateral = %s", *message.Contents.Subaccount.FreeCollateral)
	}
	if len(message.Contents.Subaccount.OpenPerpetualPositions) != 1 {
		t.Fatalf("got %d positions, want 1", len(message.Contents.Subaccount.OpenPerpetualPositions))
	}
}

func TestDecodeWsChannelDataFill(t *testing.T) {
	message, err := decodeSubaccountWsMessage(loadFixture(t, "ws_channel_data_fill.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if message.Type != wsTypeChannelData {
		t.Fatalf("type = %q, want %q", message.Type, wsTypeChannelData)
	}
	if len(message.Contents.Fills) != 1 {
		t.Fatalf("got %d fills, want 1", len(message.Contents.Fills))
	}
	if *message.Contents.Fills[0].ID != "9a5f8a0e-1c4b-4c2f-9f2a-6d4a1b0c7e31" {
		t.Fatalf("fill id = %s", *message.Contents.Fills[0].ID)
	}
	// Incremental updates send positions as an array, not a ticker-keyed map.
	if len(message.Contents.PerpetualPositions) != 1 {
		t.Fatalf("got %d positions, want 1", len(message.Contents.PerpetualPositions))
	}
	if *message.Contents.PerpetualPositions[0].Size != "0.600" {
		t.Fatalf("position size = %s", *message.Contents.PerpetualPositions[0].Size)
	}
}

func TestDecodeWsChannelDataOrderExpired(t *testing.T) {
	message, err := decodeSubaccountWsMessage(loadFixture(t, "ws_channel_data_order_expired.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(message.Contents.Orders) != 1 {
		t.Fatalf("got %d orders, want 1", len(message.Contents.Orders))
	}
	order := message.Contents.Orders[0]
	if *order.ClientID != "1700000042" {
		t.Fatalf("clientId = %s", *order.ClientID)
	}
	if *order.Status != orderStatusCanceled || *order.RemovalReason != removalReasonExpired {
		t.Fatalf("status/reason = %s/%s", *order.Status, *order.RemovalReason)
	}
}

func TestDecodeWsMessageRejectsUnknownAndErrorFrames(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "unknown type", body: `{"type":"something_new","channel":"v4_subaccounts"}`},
		{name: "missing type", body: `{"channel":"v4_subaccounts"}`},
		{name: "venue error", body: `{"type":"error","message":"Invalid subscription id"}`},
		{name: "wrong channel", body: `{"type":"channel_data","channel":"v4_trades","contents":{}}`},
		{name: "missing contents", body: `{"type":"subscribed","channel":"v4_subaccounts"}`},
		{
			name: "fill without id",
			body: `{"type":"channel_data","channel":"v4_subaccounts","contents":{"fills":[
				{"side":"BUY","price":"1","size":"1","createdAt":"2026-07-25T11:05:03.421Z"}]}}`,
		},
		{
			name: "fill with unknown side",
			body: `{"type":"channel_data","channel":"v4_subaccounts","contents":{"fills":[
				{"id":"a","side":"LONG","price":"1","size":"1","createdAt":"2026-07-25T11:05:03.421Z"}]}}`,
		},
		{
			name: "position with unknown side",
			body: `{"type":"channel_data","channel":"v4_subaccounts","contents":{"perpetualPositions":[
				{"market":"ETH-USD","side":"UP","size":"1"}]}}`,
		},
		{
			name: "order with non-numeric client id",
			body: `{"type":"channel_data","channel":"v4_subaccounts","contents":{"orders":[
				{"clientId":"abc","status":"CANCELED"}]}}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := decodeSubaccountWsMessage([]byte(testCase.body)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDecodeWsControlFrames(t *testing.T) {
	for _, body := range []string{
		`{"type":"connected","connection_id":"abc","message_id":0}`,
		`{"type":"pong"}`,
		`{"type":"unsubscribed","channel":"v4_subaccounts","id":"x/0"}`,
	} {
		message, err := decodeSubaccountWsMessage([]byte(body))
		if err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if message.Contents != nil {
			t.Fatalf("control frame %s must carry no contents", body)
		}
	}
}

func TestDecodeFills(t *testing.T) {
	response := decodeFixture[fillsResponse](t, "fills.json")
	if len(*response.Fills) != 2 {
		t.Fatalf("got %d fills, want 2", len(*response.Fills))
	}
	if *(*response.Fills)[1].Side != fillSideSell {
		t.Fatalf("second fill side = %s", *(*response.Fills)[1].Side)
	}
}

func TestDecodeCometStatus(t *testing.T) {
	response := decodeFixture[cometStatusResponse](t, "comet_status.json")
	height, err := response.height()
	if err != nil {
		t.Fatalf("height: %v", err)
	}
	if height != 38102400 {
		t.Fatalf("height = %d, want 38102400", height)
	}
}

func TestCometStatusRejectsCatchingUpAndMalformedHeight(t *testing.T) {
	catchingUp := `{"result":{"sync_info":{"latest_block_height":"100","catching_up":true}}}`
	var response cometStatusResponse
	if err := json.Unmarshal([]byte(catchingUp), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// A syncing node reports a height behind the tip; good_til_block derived
	// from it would already be in the past.
	if err := response.validate(); err == nil {
		t.Fatal("expected an error while the node is catching up")
	}

	var missing cometStatusResponse
	if err := json.Unmarshal([]byte(`{"result":{"sync_info":{}}}`), &missing); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := missing.validate(); err == nil {
		t.Fatal("expected an error for a missing latest_block_height")
	}
}

func TestDecodeBroadcastTxResponses(t *testing.T) {
	accepted := `{"jsonrpc":"2.0","id":-1,"result":{"code":0,"data":"","log":"","codespace":"","hash":"ABC"}}`
	var response broadcastTxResponse
	if err := json.Unmarshal([]byte(accepted), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if *response.Result.Code != txCodeOK {
		t.Fatalf("code = %d, want %d", *response.Result.Code, txCodeOK)
	}

	rejected := `{"jsonrpc":"2.0","id":-1,"result":{"code":2003,"codespace":"clob",` +
		`"log":"Post-only order would cross one or more maker orders","hash":"DEF"}}`
	var rejection broadcastTxResponse
	if err := json.Unmarshal([]byte(rejected), &rejection); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// A venue rejection is a well-formed response, not a decode failure.
	if err := rejection.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if *rejection.Result.Code != clobErrPostOnlyWouldCross || *rejection.Result.Codespace != clobCodespace {
		t.Fatalf("code/codespace = %d/%s", *rejection.Result.Code, *rejection.Result.Codespace)
	}
}

func TestBroadcastTxResponseRejectsRPCErrorAndMissingCode(t *testing.T) {
	rpcError := `{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":"tx too large"}}`
	var response broadcastTxResponse
	if err := json.Unmarshal([]byte(rpcError), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	err := response.validate()
	if err == nil {
		t.Fatal("expected an error for a JSON-RPC error envelope")
	}
	if !strings.Contains(err.Error(), "tx too large") {
		t.Fatalf("error should surface the rpc data: %v", err)
	}

	var missing broadcastTxResponse
	if err := json.Unmarshal([]byte(`{"result":{}}`), &missing); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := missing.validate(); err == nil {
		t.Fatal("expected an error for a result without a code")
	}
}

func TestDecodeAbciQueryResponse(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":-1,"result":{"response":{"code":0,"log":"","value":"CgIIAQ=="}}}`
	var response abciQueryResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if *response.Result.Response.Value != "CgIIAQ==" {
		t.Fatalf("value = %s", *response.Result.Response.Value)
	}
}

func TestAbciQueryResponseRejectsFailures(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "abci error code", body: `{"result":{"response":{"code":22,"log":"account not found"}}}`},
		{name: "rpc error", body: `{"error":{"code":-32603,"message":"Internal error"}}`},
		{name: "missing value", body: `{"result":{"response":{"code":0}}}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var response abciQueryResponse
			if err := json.Unmarshal([]byte(testCase.body), &response); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := response.validate(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
