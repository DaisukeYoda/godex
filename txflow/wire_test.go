package txflow

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPerpMetaResolvesByIndexFieldNotPosition(t *testing.T) {
	var response perpMetaResponse
	if err := json.Unmarshal(loadFixture(t, "perp_meta.json"), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := response.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	asset, err := resolveAssetMeta(&response, testMarket)
	if err != nil {
		t.Fatalf("resolveAssetMeta: %v", err)
	}
	if asset.index != testAssetIndex || asset.sizeStep.String() != "0.001" || asset.priceTick.String() != "0.01" {
		t.Errorf("asset = %+v", asset)
	}
}

func TestPerpMetaRequiresItsFields(t *testing.T) {
	for _, missing := range []string{"index", "szDecimals", "basePrecision", "priceTick", "name"} {
		body := strings.Replace(`{"universe":[{"name":"X-USDC","index":1,"szDecimals":2,"basePrecision":"0.01","priceTick":"0.1","maxLeverage":5}]}`,
			`"`+missing+`":`, `"_":`, 1)
		var response perpMetaResponse
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := response.validate(); err == nil || !strings.Contains(err.Error(), missing) {
			t.Errorf("without %s: validate = %v", missing, err)
		}
	}
}

func TestOrderStatusVocabularyIsClosed(t *testing.T) {
	for _, status := range []string{"Open", "PartialFilled", "Filled", "Triggered", "Canceled", "PartialCanceled", "Rejected"} {
		if !isKnownOrderStatus(status) {
			t.Errorf("%q should be known", status)
		}
	}
	// The venue's lowerCamel spelling appears only in historicalOrders'
	// top-level status, which the adapter does not read; the order's own
	// status is capitalized, and anything else is refused.
	for _, status := range []string{"filled", "canceled", "open", "Liquidated", ""} {
		if isKnownOrderStatus(status) {
			t.Errorf("%q should not be known", status)
		}
	}
	var entry historicalOrderEntry
	body := `{"order":{"symbol":"ETH-USDC","oid":1,"timestamp":1,"status":"Bogus"},"status":"bogus"}`
	if err := json.Unmarshal([]byte(body), &entry); err != nil {
		t.Fatal(err)
	}
	if err := entry.validate(); err == nil {
		t.Error("an unknown historicalOrders status was accepted")
	}
}

func TestExchangeStatusEntryNeedsExactlyOneOutcome(t *testing.T) {
	for body, wantErr := range map[string]bool{
		`{"resting":{"oid":1}}`:                          false,
		`{"filled":{"totalSz":"1","avgPx":"2","oid":1}}`: false,
		`{"error":"nope"}`:                               false,
		`{"resting":{"oid":1},"error":"nope"}`:           true,
		`{}`:                                             true,
		`{"resting":{}}`:                                 true,
		`{"filled":{"totalSz":"1","oid":1}}`:             true,
	} {
		var status orderStatusWire
		if err := json.Unmarshal([]byte(body), &status); err != nil {
			t.Fatal(err)
		}
		if err := status.validate(); (err != nil) != wantErr {
			t.Errorf("%s: validate = %v, wantErr %t", body, err, wantErr)
		}
	}
}

func TestWSEnvelopeAcceptsPongWithoutChannel(t *testing.T) {
	var envelope wsEnvelope
	if err := json.Unmarshal([]byte(`{"method":"PONG"}`), &envelope); err != nil {
		t.Fatal(err)
	}
	if err := envelope.validate(); err != nil {
		t.Errorf("pong rejected: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"data":{}}`), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Method, envelope.Channel = nil, nil
	if err := envelope.validate(); err == nil {
		t.Error("a frame with neither channel nor method was accepted")
	}
}
