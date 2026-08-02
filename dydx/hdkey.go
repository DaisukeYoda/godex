package dydx

// BIP-39/BIP-32 derivation of the chain's account key from a mnemonic,
// reproducing the official clients' derivation (Cosmos HD path
// m/44'/118'/0'/0/0) so a key generated in a wallet UI resolves to the same
// address here. Only private-key derivation along a fixed path is
// implemented — no xpub serialization, no public derivation.

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	bip39 "github.com/tyler-smith/go-bip39"
)

// hdHardenedOffset marks a BIP-32 hardened child index.
const hdHardenedOffset uint32 = 0x80000000

// cosmosHDPath is the Cosmos SDK coin-type 118 account path the official
// dYdX clients derive from, m/44'/118'/0'/0/0.
var cosmosHDPath = []uint32{
	44 + hdHardenedOffset,
	118 + hdHardenedOffset,
	0 + hdHardenedOffset,
	0,
	0,
}

// KeyFromMnemonic derives the account signing key at m/44'/118'/0'/0/0 from a
// BIP-39 mnemonic and returns it as Credentials-ready values: the private key
// hex and its bech32 address ("dydx1...").
//
// The mnemonic's checksum is validated (fail fast): deriving from a mistyped
// mnemonic would produce a syntactically valid but wrong key that only fails
// much later, as an on-chain authorization error.
func KeyFromMnemonic(mnemonic string) (privateKeyHex, address string, err error) {
	if !bip39.IsMnemonicValid(mnemonic) {
		return "", "", fmt.Errorf("dydx: invalid BIP-39 mnemonic")
	}
	seed := bip39.NewSeed(mnemonic, "")

	key, chainCode := hdMasterKey(seed)
	for _, index := range cosmosHDPath {
		key, chainCode, err = hdDeriveChild(key, chainCode, index)
		if err != nil {
			return "", "", err
		}
	}

	privateKey := secp256k1.PrivKeyFromBytes(key)
	address, err = deriveAddress(privateKey.PubKey().SerializeCompressed())
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(key), address, nil
}

// hdMasterKey computes the BIP-32 master key and chain code from a seed.
func hdMasterKey(seed []byte) (key, chainCode []byte) {
	mac := hmac.New(sha512.New, []byte("Bitcoin seed"))
	mac.Write(seed)
	digest := mac.Sum(nil)
	return digest[:32], digest[32:]
}

// hdDeriveChild computes one BIP-32 private child key.
func hdDeriveChild(key, chainCode []byte, index uint32) (childKey, childChainCode []byte, err error) {
	data := make([]byte, 0, 37)
	if index >= hdHardenedOffset {
		data = append(data, 0x00)
		data = append(data, key...)
	} else {
		privateKey := secp256k1.PrivKeyFromBytes(key)
		data = append(data, privateKey.PubKey().SerializeCompressed()...)
	}
	data = binary.BigEndian.AppendUint32(data, index)

	mac := hmac.New(sha512.New, chainCode)
	mac.Write(data)
	digest := mac.Sum(nil)

	// child = (IL + parent) mod n. SetByteSlice reports IL >= n and IsZero a
	// zero child; both mean the index is unusable (probability ~2^-127) and
	// BIP-32 says to reject rather than adjust.
	var sum secp256k1.ModNScalar
	overflow := sum.SetByteSlice(digest[:32])
	var parent secp256k1.ModNScalar
	parent.SetByteSlice(key)
	sum.Add(&parent)
	if overflow || sum.IsZero() {
		return nil, nil, fmt.Errorf("dydx: unusable BIP-32 child key at index %d", index)
	}
	childBytes := sum.Bytes()
	return childBytes[:], digest[32:], nil
}
