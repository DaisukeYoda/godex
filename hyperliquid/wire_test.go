package hyperliquid

import (
	"encoding/json"
	"strings"
	"testing"
)

// decodeAndValidate is the path every REST and WS payload takes: decode, then
// prove the fields the adapter reads are actually there.
func decodeAndValidate[T any, PT interface {
	*T
	validate() error
}](t *testing.T, payload string) error {
	t.Helper()
	value := PT(new(T))
	if err := json.Unmarshal([]byte(payload), value); err != nil {
		return err
	}
	return value.validate()
}

func TestMetaValidation(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"fixture", string(loadFixture(t, "meta.json")), ""},
		{"no universe", `{}`, `missing required field "universe"`},
		{"entry without szDecimals", `{"universe":[{"name":"ETH","maxLeverage":50,"marginTableId":50}]}`, `missing required field "szDecimals"`},
		{"entry without maxLeverage", `{"universe":[{"name":"ETH","szDecimals":4,"marginTableId":50}]}`, `missing required field "maxLeverage"`},
		{"entry without marginTableId", `{"universe":[{"name":"ETH","szDecimals":4,"maxLeverage":50}]}`, `missing required field "marginTableId"`},
		{"szDecimals out of range", `{"universe":[{"name":"ETH","szDecimals":9,"maxLeverage":50,"marginTableId":50}]}`, "outside 0..6"},
		{"non-positive leverage", `{"universe":[{"name":"ETH","szDecimals":4,"maxLeverage":0,"marginTableId":50}]}`, "non-positive maxLeverage"},
		{
			name:    "margin table without tiers",
			payload: `{"universe":[],"marginTables":[[50,{"description":""}]]}`,
			wantErr: `margin table 50 is missing required field "marginTiers"`,
		},
		{
			name:    "margin table with an empty tier list",
			payload: `{"universe":[],"marginTables":[[50,{"marginTiers":[]}]]}`,
			wantErr: "margin table 50 has no tiers",
		},
		{
			name:    "margin table entry that is not a pair",
			payload: `{"universe":[],"marginTables":[[50]]}`,
			wantErr: "has 1 elements, want 2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[metaResponse](t, test.payload), test.wantErr)
		})
	}
}

func TestClearinghouseStateValidation(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"fixture", string(loadFixture(t, "clearinghouse_long.json")), ""},
		{"flat fixture", string(loadFixture(t, "clearinghouse_flat.json")), ""},
		{
			name:    "no assetPositions",
			payload: `{"marginSummary":{"accountValue":"1","totalMarginUsed":"0"},"withdrawable":"1"}`,
			wantErr: `missing required field "assetPositions"`,
		},
		{
			name:    "no withdrawable",
			payload: `{"assetPositions":[],"marginSummary":{"accountValue":"1","totalMarginUsed":"0"}}`,
			wantErr: `missing required field "withdrawable"`,
		},
		{
			name:    "no accountValue",
			payload: `{"assetPositions":[],"marginSummary":{"totalMarginUsed":"0"},"withdrawable":"1"}`,
			wantErr: `missing required field "accountValue"`,
		},
		{
			name: "position without leverage",
			payload: `{"assetPositions":[{"position":{"coin":"ETH","szi":"1","unrealizedPnl":"0"}}],
			  "marginSummary":{"accountValue":"1","totalMarginUsed":"0"},"withdrawable":"1"}`,
			wantErr: `missing required field "leverage"`,
		},
		{
			name: "position without unrealizedPnl",
			payload: `{"assetPositions":[{"position":{"coin":"ETH","szi":"1","leverage":{"type":"cross"}}}],
			  "marginSummary":{"accountValue":"1","totalMarginUsed":"0"},"withdrawable":"1"}`,
			wantErr: `missing required field "unrealizedPnl"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[clearinghouseState](t, test.payload), test.wantErr)
		})
	}
}

func TestOrderStatusValidation(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"resting", `{"resting":{"oid":1}}`, ""},
		{"filled", `{"filled":{"totalSz":"1","avgPx":"2","oid":3}}`, ""},
		{"error", `{"error":"nope"}`, ""},
		{"resting without oid", `{"resting":{}}`, `missing required field "oid"`},
		{"filled without avgPx", `{"filled":{"totalSz":"1","oid":3}}`, `missing required field "avgPx"`},
		{"nothing recognized", `{"waitingForTrigger":{}}`, "0 recognized outcomes"},
		{"two outcomes", `{"resting":{"oid":1},"error":"nope"}`, "2 recognized outcomes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[orderStatusWire](t, test.payload), test.wantErr)
		})
	}
}

func TestUserFillsValidation(t *testing.T) {
	fixture := loadFixture(t, "ws_user_fills.json")
	var envelope wsEnvelope
	if err := json.Unmarshal(fixture, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if err := envelope.validate(); err != nil {
		t.Fatalf("validate envelope: %v", err)
	}
	if *envelope.Channel != channelUserFills {
		t.Fatalf("channel = %q, want %q", *envelope.Channel, channelUserFills)
	}
	if err := decodeAndValidate[wsUserFills](t, string(envelope.Data)); err != nil {
		t.Fatalf("validate fixture: %v", err)
	}

	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"no fills", `{"user":"0x1"}`, `missing required field "fills"`},
		{
			name:    "fill without tid",
			payload: `{"fills":[{"coin":"ETH","px":"1","sz":"1","side":"B","time":1,"oid":2}]}`,
			wantErr: `missing required field "tid"`,
		},
		{
			name:    "unknown side",
			payload: `{"fills":[{"coin":"ETH","px":"1","sz":"1","side":"X","time":1,"oid":2,"tid":3}]}`,
			wantErr: "unknown side",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[wsUserFills](t, test.payload), test.wantErr)
		})
	}
}

func TestOrderUpdateValidation(t *testing.T) {
	fixture := loadFixture(t, "ws_order_updates_canceled.json")
	var envelope wsEnvelope
	if err := json.Unmarshal(fixture, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	var updates []wsOrderUpdate
	if err := json.Unmarshal(envelope.Data, &updates); err != nil {
		t.Fatalf("decode updates: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("decoded %d updates, want 1", len(updates))
	}
	if err := updates[0].validate(); err != nil {
		t.Fatalf("validate fixture: %v", err)
	}

	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"no status", `{"order":{"coin":"ETH","oid":1}}`, `missing required field "status"`},
		{"no order", `{"status":"open"}`, `missing required field "order"`},
		{"order without oid", `{"order":{"coin":"ETH"},"status":"open"}`, `missing required field "oid"`},
		{"unknown status", `{"order":{"coin":"ETH","oid":1},"status":"teleported"}`, "unknown status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[wsOrderUpdate](t, test.payload), test.wantErr)
		})
	}
}

// Every documented status must be recognized: an unknown one aborts the
// connection, so a gap here would show up as spurious reconnects.
func TestKnownOrderStatusesCoverTheDocumentedSet(t *testing.T) {
	documented := []string{
		orderStatusOpen, orderStatusFilled, orderStatusTriggered,
		"canceled", "rejected", "marginCanceled", "vaultWithdrawalCanceled",
		"openInterestCapCanceled", "selfTradeCanceled", "reduceOnlyCanceled",
		"siblingFilledCanceled", "delistedCanceled", "liquidatedCanceled",
		"scheduledCancel", "tickRejected", "minTradeNtlRejected",
		"perpMarginRejected", "reduceOnlyRejected", "badAloPxRejected",
		"iocCancelRejected", "badTriggerPxRejected", "marketOrderNoLiquidityRejected",
		"positionIncreaseAtOpenInterestCapRejected", "positionFlipAtOpenInterestCapRejected",
		"tooAggressiveAtOpenInterestCapRejected", "openInterestIncreaseRejected",
		"insufficientSpotBalanceRejected", "oracleRejected", "perpMaxPositionRejected",
	}
	for _, status := range documented {
		if !isKnownOrderStatus(status) {
			t.Errorf("status %q is documented but unrecognized", status)
		}
	}
	if isKnownOrderStatus("notARealStatus") {
		t.Error("an invented status was accepted")
	}
}

func TestOrderQueryValidation(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"order", `{"status":"order"}`, ""},
		{"unknown oid", `{"status":"unknownOid"}`, ""},
		{"no status", `{}`, `missing required field "status"`},
		{"unrecognized status", `{"status":"maybe"}`, "unknown status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertValidationError(t, decodeAndValidate[orderQueryResponse](t, test.payload), test.wantErr)
		})
	}
}

func TestDecodeCancelStatus(t *testing.T) {
	tests := []struct {
		name        string
		statuses    []string
		wantMessage string
		wantErr     bool
	}{
		{"success string", []string{`"success"`}, "", false},
		{"error object", []string{`{"error":"Order was never placed, already canceled, or filled."}`},
			"Order was never placed, already canceled, or filled.", false},
		{"unknown string", []string{`"maybe"`}, "", true},
		{"wrong count", []string{`"success"`, `"success"`}, "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := make([]json.RawMessage, 0, len(test.statuses))
			for _, status := range test.statuses {
				raw = append(raw, json.RawMessage(status))
			}
			message, err := decodeCancelStatus(raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got message %q", message)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeCancelStatus: %v", err)
			}
			if message != test.wantMessage {
				t.Errorf("message = %q, want %q", message, test.wantMessage)
			}
		})
	}
}

func assertValidationError(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected a validation error mentioning %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want one mentioning %q", err, want)
	}
}
