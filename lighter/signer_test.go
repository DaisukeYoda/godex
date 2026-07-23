package lighter

import (
	"encoding/json"
	"testing"
	"time"

	lighterclient "github.com/elliottech/lighter-go/client"
	lightertypes "github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
)

// Offline signing tests: lighter-go is pure Go, so a throwaway key from
// GenerateAPIKey exercises the real signer without any network.

const (
	testSignerAccountIndex = int64(48)
	testSignerAPIKeyIndex  = uint8(2)
)

func newOfflineSigner(t *testing.T) *txSigner {
	t.Helper()
	privateKey, _, err := lighterclient.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	// The base URL is only contacted by check(), which these tests never call.
	signer, err := newTxSigner("http://127.0.0.1:0", privateKey, testSignerAccountIndex, testSignerAPIKeyIndex, testnetChainID)
	if err != nil {
		t.Fatalf("newTxSigner: %v", err)
	}
	return signer
}

func testCreateOrderParams() createOrderParams {
	return createOrderParams{
		marketIndex:      2,
		clientOrderIndex: 1_783_182_472_088,
		baseAmount:       200,
		price:            82_235,
		isAsk:            false,
		postOnly:         true,
		reduceOnly:       false,
		orderExpiryAt:    1_785_601_477_198,
		nonce:            7,
	}
}

func TestSignCreateOrder(t *testing.T) {
	signer := newOfflineSigner(t)
	txType, txInfo, err := signer.signCreateOrder(testCreateOrderParams())
	if err != nil {
		t.Fatalf("signCreateOrder: %v", err)
	}
	if txType != txtypes.TxTypeL2CreateOrder {
		t.Fatalf("txType = %d, want %d", txType, txtypes.TxTypeL2CreateOrder)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(txInfo), &decoded); err != nil {
		t.Fatalf("tx_info is not JSON: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("tx_info decoded empty")
	}
}

func TestSignCancelOrder(t *testing.T) {
	signer := newOfflineSigner(t)
	txType, txInfo, err := signer.signCancelOrder(2, 1_783_182_472_088, 8)
	if err != nil {
		t.Fatalf("signCancelOrder: %v", err)
	}
	if txType != txtypes.TxTypeL2CancelOrder {
		t.Fatalf("txType = %d, want %d", txType, txtypes.TxTypeL2CancelOrder)
	}
	if txInfo == "" {
		t.Fatal("empty tx_info")
	}
}

// The adoption gate requires deterministic signing. Schnorr signatures embed
// a random scalar, so signature bytes differ per run — but the signed message
// hash (GetTxHash / Hash) is fully determined by the transaction inputs and
// is what the server verifies. The gate therefore pins hash equality.
func TestSigningHashIsDeterministic(t *testing.T) {
	privateKey, _, err := lighterclient.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	client, err := lighterclient.NewTxClient(nil, privateKey, testSignerAccountIndex, testSignerAPIKeyIndex, testnetChainID)
	if err != nil {
		t.Fatalf("NewTxClient: %v", err)
	}
	build := func() *txtypes.L2CreateOrderTxInfo {
		params := testCreateOrderParams()
		nonce := params.nonce
		info, err := client.GetCreateOrderTransaction(&lightertypes.CreateOrderTxReq{
			MarketIndex:      params.marketIndex,
			ClientOrderIndex: params.clientOrderIndex,
			BaseAmount:       params.baseAmount,
			Price:            params.price,
			IsAsk:            0,
			Type:             txtypes.LimitOrder,
			TimeInForce:      txtypes.PostOnly,
			ReduceOnly:       0,
			TriggerPrice:     txtypes.NilOrderTriggerPrice,
			OrderExpiry:      params.orderExpiryAt,
		}, &lightertypes.TransactOpts{Nonce: &nonce, ExpiredAt: 1_783_183_000_000})
		if err != nil {
			t.Fatalf("GetCreateOrderTransaction: %v", err)
		}
		return info
	}
	first, second := build(), build()
	if first.GetTxHash() == "" {
		t.Fatal("signed tx must expose its hash")
	}
	if first.GetTxHash() != second.GetTxHash() {
		t.Fatalf("tx hash not deterministic: %s != %s", first.GetTxHash(), second.GetTxHash())
	}
	firstHash, err := first.Hash(testnetChainID)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	secondHash, err := second.Hash(testnetChainID)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if string(firstHash) != string(secondHash) {
		t.Fatal("message hash not deterministic")
	}
}

func TestCreateAuthToken(t *testing.T) {
	signer := newOfflineSigner(t)
	token, err := signer.createAuthToken(time.UnixMilli(1_783_182_000_000).Add(authTokenTTL))
	if err != nil {
		t.Fatalf("createAuthToken: %v", err)
	}
	if token == "" {
		t.Fatal("empty auth token")
	}
}

func TestNewTxSignerRejectsBadKey(t *testing.T) {
	if _, err := newTxSigner("http://127.0.0.1:0", "zz-not-hex", testSignerAccountIndex, testSignerAPIKeyIndex, testnetChainID); err == nil {
		t.Fatal("expected error for invalid private key")
	}
	if _, err := newTxSigner("", "abcd", testSignerAccountIndex, testSignerAPIKeyIndex, testnetChainID); err == nil {
		t.Fatal("expected error for empty base URL")
	}
}
