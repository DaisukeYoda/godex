package dydx

import (
	"crypto/sha256"
	"fmt"
	"strings"

	//nolint:staticcheck // RIPEMD-160 is deprecated for new designs but is
	// mandated by the Cosmos SDK address derivation godex must reproduce.
	"golang.org/x/crypto/ripemd160"
)

// Cosmos addresses are bech32 strings over a 20-byte digest of the public key.
// This file implements just enough of BIP-173 to encode one; godex never needs
// to decode a foreign address.

// bech32Charset is the BIP-173 data charset, indexed by 5-bit value.
const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

// bech32Generator is the BIP-173 checksum generator polynomial.
var bech32Generator = [5]uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}

// bech32Polymod is the BIP-173 checksum step function.
func bech32Polymod(values []byte) uint32 {
	checksum := uint32(1)
	for _, value := range values {
		top := checksum >> 25
		checksum = (checksum&0x1ffffff)<<5 ^ uint32(value)
		for bit := range 5 {
			if top>>uint(bit)&1 == 1 {
				checksum ^= bech32Generator[bit]
			}
		}
	}
	return checksum
}

// bech32HRPExpand expands the human-readable part for checksum purposes.
func bech32HRPExpand(hrp string) []byte {
	expanded := make([]byte, 0, len(hrp)*2+1)
	for index := 0; index < len(hrp); index++ {
		expanded = append(expanded, hrp[index]>>5)
	}
	expanded = append(expanded, 0)
	for index := 0; index < len(hrp); index++ {
		expanded = append(expanded, hrp[index]&31)
	}
	return expanded
}

// convertBits regroups 8-bit input into 5-bit output, padding the final group.
func convertBits(data []byte) []byte {
	var accumulator uint32
	var bits uint8
	converted := make([]byte, 0, len(data)*8/5+1)
	for _, value := range data {
		accumulator = accumulator<<8 | uint32(value)
		bits += 8
		for bits >= 5 {
			bits -= 5
			converted = append(converted, byte(accumulator>>uint(bits)&31))
		}
	}
	if bits > 0 {
		converted = append(converted, byte(accumulator<<uint(5-bits)&31))
	}
	return converted
}

// bech32Encode encodes payload under the human-readable prefix hrp.
func bech32Encode(hrp string, payload []byte) (string, error) {
	if hrp == "" {
		return "", fmt.Errorf("dydx: bech32 human-readable part is required")
	}
	data := convertBits(payload)
	checksumInput := append(bech32HRPExpand(hrp), data...)
	checksumInput = append(checksumInput, 0, 0, 0, 0, 0, 0)
	checksum := bech32Polymod(checksumInput) ^ 1

	var builder strings.Builder
	builder.WriteString(hrp)
	builder.WriteByte('1')
	for _, value := range data {
		builder.WriteByte(bech32Charset[value])
	}
	for index := range 6 {
		builder.WriteByte(bech32Charset[checksum>>uint(5*(5-index))&31])
	}
	return builder.String(), nil
}

// deriveAddress returns the bech32 account address for a compressed secp256k1
// public key, using the standard Cosmos derivation: ripemd160(sha256(pubkey)),
// bech32-encoded under the chain's prefix ("dydx").
func deriveAddress(compressedPubKey []byte) (string, error) {
	if len(compressedPubKey) != compressedPubKeyLen {
		return "", fmt.Errorf("dydx: compressed public key must be %d bytes, got %d",
			compressedPubKeyLen, len(compressedPubKey))
	}
	shaSum := sha256.Sum256(compressedPubKey)
	hasher := ripemd160.New()
	if _, err := hasher.Write(shaSum[:]); err != nil {
		return "", fmt.Errorf("dydx: ripemd160: %w", err)
	}
	return bech32Encode(addressPrefix, hasher.Sum(nil))
}
