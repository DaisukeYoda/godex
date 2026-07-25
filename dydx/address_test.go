package dydx

import (
	"encoding/hex"
	"strings"
	"testing"
)

// TestBech32EncodeMatchesBIP173Vectors pins the checksum and charset against
// the BIP-173 specification's own valid examples.
func TestBech32EncodeMatchesBIP173Vectors(t *testing.T) {
	for _, testCase := range []struct {
		hrp  string
		want string
	}{
		{hrp: "a", want: "a12uel5l"},
		{hrp: "?", want: "?1ezyfcl"},
	} {
		encoded, err := bech32Encode(testCase.hrp, nil)
		if err != nil {
			t.Fatalf("bech32Encode(%q): %v", testCase.hrp, err)
		}
		if encoded != testCase.want {
			t.Fatalf("bech32Encode(%q) = %q, want %q", testCase.hrp, encoded, testCase.want)
		}
	}
}

func TestBech32EncodeRejectsEmptyHRP(t *testing.T) {
	if _, err := bech32Encode("", []byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for an empty human-readable part")
	}
}

// TestDeriveAddress pins the full Cosmos derivation — ripemd160(sha256(pubkey))
// bech32-encoded under "dydx" — against vectors produced by an independent
// implementation. The first vector is the secp256k1 generator point, whose
// hash160 is also the payload of the BIP-173 P2WPKH example
// bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4, so the shared "w508d6qejxtdg..."
// data section cross-checks the hashing and the 8-to-5-bit regrouping against
// a published value.
func TestDeriveAddress(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		pubKeyHex    string
		wantAddress  string
		wantDataPart string
	}{
		{
			name:         "generator point",
			pubKeyHex:    "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			wantAddress:  "dydx1w508d6qejxtdg4y5r3zarvary0c5xw7knye700",
			wantDataPart: "w508d6qejxtdg4y5r3zarvary0c5xw7k",
		},
		{
			name:        "arbitrary key",
			pubKeyHex:   "024e3b81af9c2234cad09d679ce6035ed1392347ce64ce405f5dcd36228a25de6e",
			wantAddress: "dydx1nduq8yy8h4nr7g9vuuglzklqatmaquq9zm99js",
		},
		{
			name:        "second arbitrary key",
			pubKeyHex:   "02434831bc12821691df6e21e22072da6d8832a3dbc73b694d69159b801fa72a22",
			wantAddress: "dydx1tw5zd8wefzwd28pnja2n2mn0yalf68jjttrkdu",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pubKey, err := hex.DecodeString(testCase.pubKeyHex)
			if err != nil {
				t.Fatalf("decode pubkey: %v", err)
			}
			address, err := deriveAddress(pubKey)
			if err != nil {
				t.Fatalf("deriveAddress: %v", err)
			}
			if address != testCase.wantAddress {
				t.Fatalf("deriveAddress = %q, want %q", address, testCase.wantAddress)
			}
			if testCase.wantDataPart != "" && !strings.Contains(address, testCase.wantDataPart) {
				t.Fatalf("address %q does not contain the published data part %q",
					address, testCase.wantDataPart)
			}
		})
	}
}

func TestDeriveAddressRejectsWrongKeyLength(t *testing.T) {
	// An uncompressed key is the realistic mistake: right curve, wrong encoding.
	if _, err := deriveAddress(make([]byte, 65)); err == nil {
		t.Fatal("expected an error for a non-compressed public key length")
	}
}
