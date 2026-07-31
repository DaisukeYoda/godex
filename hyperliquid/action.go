package hyperliquid

// Exchange actions and their signing preimage.
//
// An action is submitted as JSON but signed over its MessagePack encoding, so
// the venue re-encodes the parsed action and compares. Field order, key
// spelling, and integer width are therefore part of the protocol, not
// serializer preferences: these types are declared in wire order, encoded
// with compact integers, and must never be replaced by Go maps (which encode
// in sorted, not declared, order).

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/DaisukeYoda/godex/decimal"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/crypto/sha3"
)

// limitOrderWire is the "limit" branch of an order's type field. The adapter
// places no trigger orders, so no other branch exists.
type limitOrderWire struct {
	Tif string `msgpack:"tif" json:"tif"`
}

type orderTypeWire struct {
	Limit limitOrderWire `msgpack:"limit" json:"limit"`
}

// orderWire is one order in an order action. The abbreviated keys are the
// venue's: a asset, b isBuy, p price, s size, r reduceOnly, t type, c cloid.
type orderWire struct {
	Asset      int           `msgpack:"a" json:"a"`
	IsBuy      bool          `msgpack:"b" json:"b"`
	Price      string        `msgpack:"p" json:"p"`
	Size       string        `msgpack:"s" json:"s"`
	ReduceOnly bool          `msgpack:"r" json:"r"`
	OrderType  orderTypeWire `msgpack:"t" json:"t"`
	// Cloid is the client order id, assigned before submission. Omitted
	// entirely when unset — an empty string is a different preimage.
	Cloid string `msgpack:"c,omitempty" json:"c,omitempty"`
}

type orderAction struct {
	Type     string      `msgpack:"type" json:"type"`
	Orders   []orderWire `msgpack:"orders" json:"orders"`
	Grouping string      `msgpack:"grouping" json:"grouping"`
}

// cancelByCloidWire cancels by client order id rather than the venue's oid.
// The cloid is known before submission, so a cancel stays possible even when
// the placing response was lost.
type cancelByCloidWire struct {
	Asset int    `msgpack:"asset" json:"asset"`
	Cloid string `msgpack:"cloid" json:"cloid"`
}

type cancelByCloidAction struct {
	Type    string              `msgpack:"type" json:"type"`
	Cancels []cancelByCloidWire `msgpack:"cancels" json:"cancels"`
}

// keccak256 is the venue's hash for both the action preimage and EIP-712.
func keccak256(chunks ...[]byte) [32]byte {
	hasher := sha3.NewLegacyKeccak256()
	for _, chunk := range chunks {
		hasher.Write(chunk)
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

// encodeAction MessagePack-encodes an action with compact integers, matching
// the reference implementation's encoder settings.
func encodeAction(action any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.UseCompactInts(true)
	if err := encoder.Encode(action); err != nil {
		return nil, fmt.Errorf("hyperliquid: encoding action failed: %w", err)
	}
	return buffer.Bytes(), nil
}

// actionHash builds the connection id the phantom agent is signed over:
// msgpack(action) ‖ nonce(8, big endian) ‖ vault marker ‖ optional expiry.
// The vault marker is 0x00 for no vault, or 0x01 followed by the 20 address
// bytes. expiresAfter, when present, is a 0x00 separator plus 8 big-endian
// bytes.
func actionHash(action any, vaultAddress []byte, nonce uint64, expiresAfter *uint64) ([32]byte, error) {
	encoded, err := encodeAction(action)
	if err != nil {
		return [32]byte{}, err
	}
	if length := len(vaultAddress); length != 0 && length != addressLen {
		return [32]byte{}, fmt.Errorf("hyperliquid: vault address must be %d bytes, got %d", addressLen, length)
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
	return keccak256(preimage), nil
}

// wireDecimal renders a decimal the way the venue's own clients do: the
// shortest exact form, with trailing fractional zeros and a bare trailing
// point removed. "100.00" and "100" are the same number but different signing
// preimages, and only the latter is what the venue re-encodes.
func wireDecimal(value decimal.Decimal) string {
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
