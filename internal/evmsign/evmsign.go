// Package evmsign holds the signing primitives shared by the venues that
// authenticate exchange actions the Hyperliquid way: an action is
// MessagePack-encoded, framed with its nonce and vault marker, keccak-hashed
// into a "connection id", and that id is signed as EIP-712 typed data with a
// secp256k1 key. Everything here is a pure function over bytes; the venue
// packages own the EIP-712 Agent struct that differs between them.
package evmsign

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/DaisukeYoda/godex/decimal"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/crypto/sha3"
)

// AddressLen is the byte length of an EVM-style address.
const AddressLen = 20

// compactSigLen is the length of dcrd's recoverable signature: one recovery
// byte followed by R‖S.
const compactSigLen = 65

var eip712DomainTypeHash = Keccak256([]byte(
	"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))

// Signature is the venue's signature envelope. R and S are minimal-form hex
// quantities (no zero padding), matching the reference clients.
type Signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V uint8  `json:"v"`
}

// KeySigner signs digests with a raw secp256k1 key held in process. Use a
// venue-scoped API (agent) wallet: it can trade but cannot withdraw or
// transfer, so a leaked trading process cannot move funds. The master key
// must never reach this process.
type KeySigner struct {
	privateKey *secp256k1.PrivateKey
	walletAddr string
}

// NewKeySigner parses a hex-encoded secp256k1 private key ("0x" prefix
// optional).
func NewKeySigner(privateKeyHex string) (*KeySigner, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(privateKeyHex), "0x")
	keyBytes, err := hex.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("private key is not valid hex: %w", err)
	}
	if len(keyBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("private key must be %d bytes, got %d",
			secp256k1.PrivKeyBytesLen, len(keyBytes))
	}
	// PrivKeyFromBytes silently clamps out-of-range scalars, so reject them
	// here: a key the venue would not recognize must fail loudly at
	// construction, not produce signatures for the wrong address.
	var scalar secp256k1.ModNScalar
	if overflow := scalar.SetByteSlice(keyBytes); overflow || scalar.IsZero() {
		return nil, errors.New("private key is outside the secp256k1 group order")
	}
	privateKey := secp256k1.NewPrivateKey(&scalar)
	return &KeySigner{
		privateKey: privateKey,
		walletAddr: DeriveAddress(privateKey.PubKey()),
	}, nil
}

// Address is the lowercase 0x address of the signing wallet.
func (s *KeySigner) Address() string { return s.walletAddr }

// SignDigest produces the recoverable (r, s, v) signature over an EIP-712
// digest. dcrd's signer is RFC 6979 deterministic and canonicalizes S to the
// lower half of the group order, which is what EIP-2 requires; SignCompact
// returns the recovery byte already offset by 27 when told the key is
// uncompressed, which is Ethereum's v convention.
func (s *KeySigner) SignDigest(digest [32]byte) (Signature, error) {
	compact := ecdsa.SignCompact(s.privateKey, digest[:], false)
	if len(compact) != compactSigLen {
		return Signature{}, fmt.Errorf("unexpected compact signature length %d", len(compact))
	}
	return Signature{
		R: HexQuantity(compact[1:33]),
		S: HexQuantity(compact[33:65]),
		V: compact[0],
	}, nil
}

// Keccak256 is the hash used for both the action preimage and EIP-712.
func Keccak256(chunks ...[]byte) [32]byte {
	hasher := sha3.NewLegacyKeccak256()
	for _, chunk := range chunks {
		hasher.Write(chunk)
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

// EIP712DomainSeparator hashes an EIP712Domain{name, version, chainId,
// verifyingContract}.
func EIP712DomainSeparator(name, version string, chainID int64, verifyingContract [AddressLen]byte) [32]byte {
	nameHash := Keccak256([]byte(name))
	versionHash := Keccak256([]byte(version))
	chainWord := LeftPad32(big.NewInt(chainID).Bytes())
	contractWord := LeftPad32(verifyingContract[:])
	return Keccak256(eip712DomainTypeHash[:], nameHash[:], versionHash[:], chainWord[:], contractWord[:])
}

// EIP712Digest is the final signing digest: 0x1901 ‖ domain ‖ structHash.
func EIP712Digest(domainSeparator, structHash [32]byte) [32]byte {
	return Keccak256([]byte{0x19, 0x01}, domainSeparator[:], structHash[:])
}

// LeftPad32 left-pads a big-endian integer to an ABI word.
func LeftPad32(value []byte) [32]byte {
	var word [32]byte
	copy(word[32-len(value):], value)
	return word
}

// EncodeAction MessagePack-encodes an action with compact integers, matching
// the reference implementation's encoder settings. Actions must be structs
// declared in wire order, never maps (which encode in sorted order).
func EncodeAction(action any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(action); err != nil {
		return nil, fmt.Errorf("encoding action failed: %w", err)
	}
	return buffer.Bytes(), nil
}

// ActionHash builds the connection id the phantom agent is signed over:
// msgpack(action) ‖ nonce(8, big endian) ‖ vault marker ‖ optional expiry.
// The vault marker is 0x00 for no vault, or 0x01 followed by the 20 address
// bytes. expiresAfter, when present, is a 0x00 separator plus 8 big-endian
// bytes.
func ActionHash(action any, vaultAddress []byte, nonce uint64, expiresAfter *uint64) ([32]byte, error) {
	encoded, err := EncodeAction(action)
	if err != nil {
		return [32]byte{}, err
	}
	if length := len(vaultAddress); length != 0 && length != AddressLen {
		return [32]byte{}, fmt.Errorf("vault address must be %d bytes, got %d", AddressLen, length)
	}

	preimage := make([]byte, 0, len(encoded)+len(vaultAddress)+18)
	preimage = append(preimage, encoded...)
	preimage = binary.BigEndian.AppendUint64(preimage, nonce)
	if len(vaultAddress) == 0 {
		preimage = append(preimage, 0x00)
	} else {
		preimage = append(preimage, 0x01)
		preimage = append(preimage, vaultAddress...)
	}
	if expiresAfter != nil {
		preimage = append(preimage, 0x00)
		preimage = binary.BigEndian.AppendUint64(preimage, *expiresAfter)
	}
	return Keccak256(preimage), nil
}

// WireDecimal renders a decimal the way the venues' own clients do: the
// shortest exact form, with trailing fractional zeros and a bare trailing
// point removed. "100.00" and "100" are the same number but different signing
// preimages, and only the latter is what the venue re-encodes.
func WireDecimal(value decimal.Decimal) string {
	text := value.String()
	if !strings.Contains(text, ".") {
		return text
	}
	text = strings.TrimRight(text, "0")
	text = strings.TrimSuffix(text, ".")
	if text == "" || text == "-" {
		return "0"
	}
	return text
}

// HexQuantity renders big-endian bytes as a minimal-form 0x quantity, the
// form the venues' reference clients send.
func HexQuantity(value []byte) string {
	trimmed := strings.TrimLeft(hex.EncodeToString(value), "0")
	if trimmed == "" {
		return "0x0"
	}
	return "0x" + trimmed
}

// DeriveAddress returns the lowercase 0x address of a public key: the last 20
// bytes of the keccak hash of its uncompressed encoding, minus the 0x04 tag.
func DeriveAddress(publicKey *secp256k1.PublicKey) string {
	uncompressed := publicKey.SerializeUncompressed()
	digest := Keccak256(uncompressed[1:])
	return "0x" + hex.EncodeToString(digest[12:])
}

// ParseAddress decodes a 0x-prefixed EVM-style address.
func ParseAddress(value string) ([]byte, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(value), "0x")
	decoded, err := hex.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("address %q is not valid hex: %w", value, err)
	}
	if len(decoded) != AddressLen {
		return nil, fmt.Errorf("address %q must be %d bytes, got %d", value, AddressLen, len(decoded))
	}
	return decoded, nil
}

// NormalizeAddress renders address bytes in the lowercase 0x form the venues
// echo back, so equality checks against streamed payloads are exact rather
// than case-sensitive comparisons of user input.
func NormalizeAddress(address []byte) string {
	return "0x" + hex.EncodeToString(address)
}
