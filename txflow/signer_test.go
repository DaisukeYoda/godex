package txflow

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/DaisukeYoda/godex/internal/evmsign"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/vmihailenco/msgpack/v5"
)

// The venue publishes no signing test vectors (its platform API documentation
// is still to come). What can be pinned without them: the exact preimage the
// venue's own client was observed to build, and that a signature recovers to
// the signing wallet under the venue's domain. The hex constants below are the
// implementation's own output, kept so a change to the preimage or domain
// fails here rather than on a live order — they are NOT venue-verified.

var testSigningParams = SigningParams{Network: mainnetSigningNetwork, ChainID: mainnetSigningChainID, APIVersion: mainnetSigningAPIVersion}

func referenceOrderAction() orderAction {
	return orderAction{
		Type:     actionTypeOrder,
		Grouping: groupingNA,
		Orders: []orderWire{{
			Asset: 1, IsBuy: true, Price: "64192.9", Size: "0.0465", ReduceOnly: false,
			OrderType: orderTypeWire{Limit: limitOrderWire{Tif: tifALO}},
		}},
	}
}

// The venue's client MessagePack-encodes {grouping, orders} — the action
// without its type, grouping first — and that is what it signs.
func TestOrderActionPreimageOmitsTypeAndKeepsWireOrder(t *testing.T) {
	encoded, err := evmsign.EncodeAction(referenceOrderAction())
	if err != nil {
		t.Fatalf("EncodeAction: %v", err)
	}
	// 0x82: a two-entry map; 0xa8 "grouping"; 0xa2 "na"; 0xa6 "orders".
	wantPrefix := "82a867726f7570696e67a26e61a66f7264657273"
	if got := hex.EncodeToString(encoded); !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("preimage = %s, want prefix %s", got, wantPrefix)
	}
	if strings.Contains(hex.EncodeToString(encoded), hex.EncodeToString([]byte("type"))) {
		t.Errorf("preimage carries the action type: %x", encoded)
	}
	// The order's own keys keep the a,b,p,s,r,t order and the asset stays an
	// integer.
	var decoded map[string]any
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode preimage: %v", err)
	}
	orders := decoded["orders"].([]any)
	order := orders[0].(map[string]any)
	if _, isInt := order["a"].(int8); !isInt {
		t.Errorf("asset encoded as %T, want an integer", order["a"])
	}
	cancel, err := evmsign.EncodeAction(cancelAction{Type: actionTypeCancel, Cancels: []cancelWire{{Asset: 1, Oid: 215841473334}}})
	if err != nil {
		t.Fatalf("EncodeAction(cancel): %v", err)
	}
	if got := hex.EncodeToString(cancel); !strings.HasPrefix(got, "81a763616e63656c73") { // 0x81 map(1) "cancels"
		t.Errorf("cancel preimage = %s, want a one-key {cancels} map", got)
	}
}

func TestSignatureRecoversToTheSigningWallet(t *testing.T) {
	sgnr, err := newKeySigner(testPrivateKey, testSigningParams)
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	const nonce = 1787051274965
	sig, err := sgnr.signAction(referenceOrderAction(), nonce)
	if err != nil {
		t.Fatalf("signAction: %v", err)
	}
	connectionID, err := evmsign.ActionHash(referenceOrderAction(), nil, nonce, nil)
	if err != nil {
		t.Fatalf("ActionHash: %v", err)
	}
	digest := agentDigest(testSigningParams, connectionID)

	compact := make([]byte, 65)
	compact[0] = sig.V
	copy(compact[33-len(unhex(t, sig.R)):33], unhex(t, sig.R))
	copy(compact[65-len(unhex(t, sig.S)):65], unhex(t, sig.S))
	pub, _, err := ecdsa.RecoverCompact(compact, digest[:])
	if err != nil {
		t.Fatalf("RecoverCompact: %v", err)
	}
	if got := evmsign.DeriveAddress(pub); got != sgnr.address() {
		t.Errorf("signature recovers to %s, want %s", got, sgnr.address())
	}
	if sig.V != 27 && sig.V != 28 {
		t.Errorf("v = %d, want Ethereum's 27/28 convention", sig.V)
	}
}

// Pins the implemented domain and Agent struct hashing. Not venue-verified;
// see the file comment.
func TestAgentDigestIsStable(t *testing.T) {
	var connectionID [32]byte
	for i := range connectionID {
		connectionID[i] = byte(i)
	}
	got := hex.EncodeToString(func() []byte { d := agentDigest(testSigningParams, connectionID); return d[:] }())
	// The Agent type string and the domain are hashed per EIP-712 with the
	// two uint32 fields as ABI words; recompute the digest here by hand to
	// pin the structure independently of agentDigest's own arithmetic.
	typeHash := evmsign.Keccak256([]byte("Agent(string txflowNetwork,uint32 chainId,uint32 apiVersion,bytes32 connectionId)"))
	nameHash := evmsign.Keccak256([]byte("TxFlow-Mainnet"))
	var chainWord, versionWord [32]byte
	chainWord[30], chainWord[31] = 0x03, 0x65 // 869
	versionWord[31] = 1
	structHash := evmsign.Keccak256(typeHash[:], nameHash[:], chainWord[:], versionWord[:], connectionID[:])
	domain := evmsign.EIP712DomainSeparator("TxFlow-Mainnet", "1", 869, [20]byte{})
	want := evmsign.EIP712Digest(domain, structHash)
	if got != hex.EncodeToString(want[:]) {
		t.Errorf("agentDigest = %s, want %x", got, want)
	}
	// A different network, chain id, or API version is a different digest,
	// so a signature cannot be replayed across networks.
	for _, other := range []SigningParams{
		{Network: "TxFlow-Testnet", ChainID: 869, APIVersion: 1},
		{Network: "TxFlow-Mainnet", ChainID: 868, APIVersion: 1},
		{Network: "TxFlow-Mainnet", ChainID: 869, APIVersion: 2},
	} {
		if d := agentDigest(other, connectionID); hex.EncodeToString(d[:]) == got {
			t.Errorf("params %+v produce the mainnet digest", other)
		}
	}
}

func TestNewKeySignerRejectsBadInput(t *testing.T) {
	for _, key := range []string{"", "0x12", "zz", "0x" + strings.Repeat("00", 32)} {
		if _, err := newKeySigner(key, testSigningParams); err == nil {
			t.Errorf("key %q was accepted", key)
		}
	}
	if _, err := newKeySigner(testPrivateKey, SigningParams{Network: "TxFlow-Mainnet"}); err == nil {
		t.Error("incomplete signing parameters were accepted")
	}
	sgnr, err := newKeySigner(testPrivateKey, testSigningParams)
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	priv := secp256k1.PrivKeyFromBytes(unhex(t, testPrivateKey))
	if want := evmsign.DeriveAddress(priv.PubKey()); sgnr.address() != want {
		t.Errorf("address = %s, want %s", sgnr.address(), want)
	}
}

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	if err != nil {
		t.Fatalf("hex %q: %v", value, err)
	}
	return decoded
}
