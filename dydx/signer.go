package dydx

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// signer produces broadcast-ready transaction bytes. The interface covers the
// whole signing operation rather than the underlying ECDSA primitive so tests
// can substitute it wholesale and inspect the order parameters that reached it.
type signer interface {
	// signPlaceOrder builds, signs, and encodes a MsgPlaceOrder transaction.
	signPlaceOrder(params placeOrderParams, envelope txParams) ([]byte, error)
	// signCancelOrder builds, signs, and encodes a MsgCancelOrder transaction.
	signCancelOrder(params cancelOrderParams, envelope txParams) ([]byte, error)
	// address is the bech32 account address the signing key controls.
	address() string
	// pubKey is the compressed public key, needed to build AuthInfo.
	pubKey() []byte
}

// keySigner signs with a raw secp256k1 key held in process. Use a scoped
// trading key registered as an on-chain authenticator; a key with withdrawal
// authority must never reach a trading process.
type keySigner struct {
	privateKey       *secp256k1.PrivateKey
	compressedPubKey []byte
	accountAddress   string
}

var _ signer = (*keySigner)(nil)

// newKeySigner parses a hex-encoded secp256k1 private key ("0x" prefix
// optional) and derives its account address.
func newKeySigner(privateKeyHex string) (*keySigner, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(privateKeyHex), "0x")
	keyBytes, err := hex.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("dydx: private key is not valid hex: %w", err)
	}
	if len(keyBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("dydx: private key must be %d bytes, got %d",
			secp256k1.PrivKeyBytesLen, len(keyBytes))
	}
	// PrivKeyFromBytes silently clamps out-of-range scalars, so reject them
	// here: a key the venue would not recognize must fail loudly at
	// construction, not produce signatures for the wrong address.
	var scalar secp256k1.ModNScalar
	if overflow := scalar.SetByteSlice(keyBytes); overflow || scalar.IsZero() {
		return nil, fmt.Errorf("dydx: private key is outside the secp256k1 group order")
	}
	privateKey := secp256k1.NewPrivateKey(&scalar)

	compressedPubKey := privateKey.PubKey().SerializeCompressed()
	accountAddress, err := deriveAddress(compressedPubKey)
	if err != nil {
		return nil, err
	}
	return &keySigner{
		privateKey:       privateKey,
		compressedPubKey: compressedPubKey,
		accountAddress:   accountAddress,
	}, nil
}

func (s *keySigner) address() string { return s.accountAddress }

func (s *keySigner) pubKey() []byte { return s.compressedPubKey }

// sign returns the raw 64-byte R||S signature Cosmos expects. dcrd's signer is
// RFC 6979 deterministic and negates S when it exceeds half the group order,
// so the result is canonically low-S; SignCompact serializes exactly R||S
// after a leading recovery byte that Cosmos does not use.
func (s *keySigner) sign(digest []byte) ([]byte, error) {
	compact := ecdsa.SignCompact(s.privateKey, digest, true)
	if len(compact) != signatureLen+1 {
		return nil, fmt.Errorf("dydx: unexpected compact signature length %d", len(compact))
	}
	return compact[1:], nil
}

func (s *keySigner) signPlaceOrder(params placeOrderParams, envelope txParams) ([]byte, error) {
	envelope.pubKeyCompressed = s.compressedPubKey
	return buildAndSignTx(msgPlaceOrderTypeURL, buildPlaceOrderMessage(params), envelope, s.sign)
}

func (s *keySigner) signCancelOrder(params cancelOrderParams, envelope txParams) ([]byte, error) {
	envelope.pubKeyCompressed = s.compressedPubKey
	return buildAndSignTx(msgCancelOrderTypeURL, buildCancelOrderMessage(params), envelope, s.sign)
}
