package dydx

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// The expected values are pinned against the official TypeScript client
// (@dydxprotocol/v4-client-js LocalWallet.fromMnemonic, cosmjs
// DirectSecp256k1HdWallet), captured 2026-08-01. Both mnemonics are standard
// BIP-39 test mnemonics, never funded.
func TestKeyFromMnemonicMatchesReferenceClient(t *testing.T) {
	cases := []struct {
		name          string
		mnemonic      string
		privateKeyHex string
		address       string
		pubKeyHex     string
	}{
		{
			name: "bip39 zero-entropy mnemonic",
			mnemonic: "abandon abandon abandon abandon abandon abandon " +
				"abandon abandon abandon abandon abandon about",
			privateKeyHex: "c4a48e2fce1481cd3294b4490f6678090ea98d3d0e5cd984558ab0968741b104",
			address:       "dydx19rl4cm2hmr8afy4kldpxz3fka4jguq0a4erelz",
			pubKeyHex:     "024f4e2ad99c34d60b9ba6283c9431a8418af8673212961f97a77b6377fcd05b62",
		},
		{
			name: "bip39 legal-winner mnemonic",
			mnemonic: "legal winner thank year wave sausage worth useful " +
				"legal winner thank yellow",
			privateKeyHex: "e0aff28c65d3d0dc3836e890414b99758a0a166291bdacd880cb1ca8a1a0e6bc",
			address:       "dydx1avgyh77ycn997ja45q5q8ss8y9mr424jfrvh4k",
			pubKeyHex:     "03510c69e626043eda293ccd3aecf49a568a9aab62173e77540fe385a454e61513",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			privateKeyHex, address, err := KeyFromMnemonic(tc.mnemonic)
			if err != nil {
				t.Fatalf("KeyFromMnemonic: %v", err)
			}
			if privateKeyHex != tc.privateKeyHex {
				t.Errorf("private key = %s, want %s", privateKeyHex, tc.privateKeyHex)
			}
			if address != tc.address {
				t.Errorf("address = %s, want %s", address, tc.address)
			}
			keyBytes, err := hex.DecodeString(privateKeyHex)
			if err != nil {
				t.Fatalf("decode private key: %v", err)
			}
			pubKey := secp256k1.PrivKeyFromBytes(keyBytes).PubKey().SerializeCompressed()
			if hex.EncodeToString(pubKey) != tc.pubKeyHex {
				t.Errorf("public key = %x, want %s", pubKey, tc.pubKeyHex)
			}
		})
	}
}

func TestKeyFromMnemonicRejectsInvalidMnemonic(t *testing.T) {
	cases := []struct {
		name     string
		mnemonic string
	}{
		{"empty", ""},
		{"wrong word count", "abandon abandon abandon"},
		{"unknown word", strings.Repeat("notaword ", 11) + "notaword"},
		// Valid words, broken checksum: the last word carries the checksum.
		{"broken checksum", "abandon abandon abandon abandon abandon abandon " +
			"abandon abandon abandon abandon abandon abandon"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := KeyFromMnemonic(tc.mnemonic); err == nil {
				t.Fatal("expected an error for an invalid mnemonic")
			}
		})
	}
}
