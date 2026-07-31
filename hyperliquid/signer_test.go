package hyperliquid

import (
	"encoding/hex"
	"testing"
)

// The vectors below are the reference implementation's own signing tests
// (hyperliquid-python-sdk, tests/signing_test.py). They pin the whole
// preimage — MessagePack field order and integer width, nonce and vault
// framing, the phantom agent's network source, and the EIP-712 domain — so a
// change in any of those fails here rather than on a live order.
const referencePrivateKey = "0x0123456789012345678901234567890123456789012345678901234567890123"

// dummyAction is the reference tests' non-trading action. It exists to pin
// the envelope (nonce and vault framing) independently of order encoding.
type dummyAction struct {
	Type string `msgpack:"type"`
	Num  uint64 `msgpack:"num"`
}

func referenceOrderAction(t *testing.T, cloid string) orderAction {
	t.Helper()
	return orderAction{
		Type: actionTypeOrder,
		Orders: []orderWire{{
			Asset:      1,
			IsBuy:      true,
			Price:      "100",
			Size:       "100",
			ReduceOnly: false,
			OrderType:  orderTypeWire{Limit: limitOrderWire{Tif: "Gtc"}},
			Cloid:      cloid,
		}},
		Grouping: groupingNA,
	}
}

func signWith(t *testing.T, source string, vault []byte, action any, nonce uint64) signature {
	t.Helper()
	sgnr, err := newKeySigner(referencePrivateKey, source, vault)
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	sig, err := sgnr.signAction(action, nonce)
	if err != nil {
		t.Fatalf("signAction: %v", err)
	}
	return sig
}

func assertSignature(t *testing.T, got signature, wantR, wantS string, wantV uint8) {
	t.Helper()
	if got.R != wantR || got.S != wantS || got.V != wantV {
		t.Errorf("signature mismatch\n got r=%s s=%s v=%d\nwant r=%s s=%s v=%d",
			got.R, got.S, got.V, wantR, wantS, wantV)
	}
}

func TestSignActionMatchesReferenceVectors(t *testing.T) {
	vault, err := parseAddress("0x1719884eb866cb12b2287399b15f7db5e7d775ea")
	if err != nil {
		t.Fatalf("parseAddress: %v", err)
	}
	// The reference test's num is float_to_int_for_hashing(1000), i.e. 1000
	// scaled by 10^8.
	dummy := dummyAction{Type: "dummy", Num: 1000 * 100_000_000}

	tests := []struct {
		name   string
		source string
		vault  []byte
		action any
		wantR  string
		wantS  string
		wantV  uint8
	}{
		{
			name: "dummy action mainnet", source: mainnetSigningSource, action: dummy,
			wantR: "0x53749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298",
			wantS: "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8",
			wantV: 27,
		},
		{
			name: "dummy action testnet", source: testnetSigningSource, action: dummy,
			wantR: "0x542af61ef1f429707e3c76c5293c80d01f74ef853e34b76efffcb57e574f9510",
			wantS: "0x17b8b32f086e8cdede991f1e2c529f5dd5297cbe8128500e00cbaf766204a613",
			wantV: 28,
		},
		{
			name: "dummy action with vault mainnet", source: mainnetSigningSource, vault: vault, action: dummy,
			wantR: "0x3c548db75e479f8012acf3000ca3a6b05606bc2ec0c29c50c515066a326239",
			wantS: "0x4d402be7396ce74fbba3795769cda45aec00dc3125a984f2a9f23177b190da2c",
			wantV: 28,
		},
		{
			name: "dummy action with vault testnet", source: testnetSigningSource, vault: vault, action: dummy,
			wantR: "0xe281d2fb5c6e25ca01601f878e4d69c965bb598b88fac58e475dd1f5e56c362b",
			wantS: "0x7ddad27e9a238d045c035bc606349d075d5c5cd00a6cd1da23ab5c39d4ef0f60",
			wantV: 27,
		},
		{
			name: "order mainnet", source: mainnetSigningSource, action: referenceOrderAction(t, ""),
			wantR: "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e",
			wantS: "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e",
			wantV: 28,
		},
		{
			name: "order testnet", source: testnetSigningSource, action: referenceOrderAction(t, ""),
			wantR: "0x82b2ba28e76b3d761093aaded1b1cdad4960b3af30212b343fb2e6cdfa4e3d54",
			wantS: "0x6b53878fc99d26047f4d7e8c90eb98955a109f44209163f52d8dc4278cbbd9f5",
			wantV: 27,
		},
		{
			name:   "order with cloid mainnet",
			source: mainnetSigningSource,
			action: referenceOrderAction(t, "0x00000000000000000000000000000001"),
			wantR:  "0x41ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3",
			wantS:  "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da",
			wantV:  27,
		},
		{
			name:   "order with cloid testnet",
			source: testnetSigningSource,
			action: referenceOrderAction(t, "0x00000000000000000000000000000001"),
			wantR:  "0xeba0664bed2676fc4e5a743bf89e5c7501aa6d870bdb9446e122c9466c5cd16d",
			wantS:  "0x7f3e74825c9114bc59086f1eebea2928c190fdfbfde144827cb02b85bbe90988",
			wantV:  28,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSignature(t, signWith(t, test.source, test.vault, test.action, 0),
				test.wantR, test.wantS, test.wantV)
		})
	}
}

// TestActionHashMatchesReferenceConnectionID pins the MessagePack preimage on
// its own, so an encoder change is reported as an encoding fault rather than
// as an unexplained signature mismatch.
func TestActionHashMatchesReferenceConnectionID(t *testing.T) {
	action := orderAction{
		Type: actionTypeOrder,
		Orders: []orderWire{{
			Asset:     4,
			IsBuy:     true,
			Price:     "1670.1",
			Size:      "0.0147",
			OrderType: orderTypeWire{Limit: limitOrderWire{Tif: tifIOC}},
		}},
		Grouping: groupingNA,
	}
	hash, err := actionHash(action, nil, 1677777606040, nil)
	if err != nil {
		t.Fatalf("actionHash: %v", err)
	}
	const want = "0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
	if got := hex.EncodeToString(hash[:]); got != want {
		t.Errorf("connection id = %s, want %s", got, want)
	}
}

func TestActionHashRejectsMalformedVaultAddress(t *testing.T) {
	if _, err := actionHash(dummyAction{Type: "dummy"}, make([]byte, 19), 0, nil); err == nil {
		t.Fatal("expected a short vault address to be rejected")
	}
}

func TestNewKeySignerRejectsBadKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"not hex", "0xzz23456789012345678901234567890123456789012345678901234567890123"},
		{"too short", "0x0123"},
		{"zero scalar", "0x" + hex.EncodeToString(make([]byte, 32))},
		{"above group order", "0x" + "FF" + "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newKeySigner(test.key, mainnetSigningSource, nil); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// TestDeriveAddressMatchesKnownVector uses the EIP-155 example key, whose
// address is published in the EIP itself.
func TestDeriveAddressMatchesKnownVector(t *testing.T) {
	const eip155Key = "0x4646464646464646464646464646464646464646464646464646464646464646"
	sgnr, err := newKeySigner(eip155Key, mainnetSigningSource, nil)
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	const want = "0x9d8a62f656a8d1615c1294fd71e9cfb3e4855a4f"
	if got := sgnr.address(); got != want {
		t.Errorf("address = %s, want %s", got, want)
	}
}

func TestParseAddressRejectsWrongLength(t *testing.T) {
	if _, err := parseAddress("0x1234"); err == nil {
		t.Fatal("expected an error for a short address")
	}
}

func TestWireDecimalStripsTrailingZeros(t *testing.T) {
	tests := []struct{ in, want string }{
		{"100", "100"},
		{"100.00", "100"},
		{"0.10000", "0.1"},
		{"1670.10", "1670.1"},
		{"0.0000", "0"},
		{"-0.500", "-0.5"},
	}
	for _, test := range tests {
		value := mustDecimal(t, test.in)
		if got := wireDecimal(value); got != test.want {
			t.Errorf("wireDecimal(%s) = %s, want %s", test.in, got, test.want)
		}
	}
}
