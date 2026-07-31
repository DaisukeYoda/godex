package hyperliquid

// L1 action signing. An action is hashed into a "phantom agent" — a
// {source, connectionId} struct — which is then signed as EIP-712 typed data
// under a fixed domain. The domain is not an EVM deployment: chain id 1337
// and the zero verifying contract are literal protocol constants, and the
// mainnet/testnet split lives in the agent's source field instead.

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// addressLen is the byte length of an EVM-style address.
const addressLen = 20

// compactSigLen is the length of dcrd's recoverable signature: one recovery
// byte followed by R‖S.
const compactSigLen = 65

var (
	eip712DomainTypeHash = keccak256([]byte(
		"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	agentTypeHash = keccak256([]byte("Agent(string source,bytes32 connectionId)"))
)

// signature is the venue's signature envelope. r and s are minimal-form hex
// quantities (no zero padding), matching the reference clients.
type signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V uint8  `json:"v"`
}

// signer produces the signature for an exchange action. The interface covers
// the whole operation rather than the ECDSA primitive so tests can substitute
// it and inspect what reached it.
type signer interface {
	// signAction signs action for nonce, honoring the configured vault
	// address.
	signAction(action any, nonce uint64) (signature, error)
	// address is the 0x address of the signing wallet.
	address() string
}

// keySigner signs with a raw secp256k1 key held in process. Use a Hyperliquid
// API (agent) wallet: it can trade but cannot withdraw or transfer, so a
// leaked trading process cannot move funds. The master key must never reach
// this process.
type keySigner struct {
	privateKey   *secp256k1.PrivateKey
	walletAddr   string
	source       string
	vaultAddress []byte
}

var _ signer = (*keySigner)(nil)

// newKeySigner parses a hex-encoded secp256k1 private key ("0x" prefix
// optional) and binds it to a network source and optional vault address.
func newKeySigner(privateKeyHex, source string, vaultAddress []byte) (*keySigner, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(privateKeyHex), "0x")
	keyBytes, err := hex.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: private key is not valid hex: %w", err)
	}
	if len(keyBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("hyperliquid: private key must be %d bytes, got %d",
			secp256k1.PrivKeyBytesLen, len(keyBytes))
	}
	// PrivKeyFromBytes silently clamps out-of-range scalars, so reject them
	// here: a key the venue would not recognize must fail loudly at
	// construction, not produce signatures for the wrong address.
	var scalar secp256k1.ModNScalar
	if overflow := scalar.SetByteSlice(keyBytes); overflow || scalar.IsZero() {
		return nil, fmt.Errorf("hyperliquid: private key is outside the secp256k1 group order")
	}
	if length := len(vaultAddress); length != 0 && length != addressLen {
		return nil, fmt.Errorf("hyperliquid: vault address must be %d bytes, got %d", addressLen, length)
	}
	privateKey := secp256k1.NewPrivateKey(&scalar)
	return &keySigner{
		privateKey:   privateKey,
		walletAddr:   deriveAddress(privateKey.PubKey()),
		source:       source,
		vaultAddress: vaultAddress,
	}, nil
}

func (s *keySigner) address() string { return s.walletAddr }

func (s *keySigner) signAction(action any, nonce uint64) (signature, error) {
	connectionID, err := actionHash(action, s.vaultAddress, nonce, nil)
	if err != nil {
		return signature{}, err
	}
	return s.sign(agentDigest(s.source, connectionID))
}

// sign produces the recoverable (r, s, v) signature over an EIP-712 digest.
// dcrd's signer is RFC 6979 deterministic and canonicalizes S to the lower
// half of the group order, which is what EIP-2 requires; SignCompact returns
// the recovery byte already offset by 27 when told the key is uncompressed,
// which is Ethereum's v convention.
func (s *keySigner) sign(digest [32]byte) (signature, error) {
	compact := ecdsa.SignCompact(s.privateKey, digest[:], false)
	if len(compact) != compactSigLen {
		return signature{}, fmt.Errorf("hyperliquid: unexpected compact signature length %d", len(compact))
	}
	return signature{
		R: hexQuantity(compact[1:33]),
		S: hexQuantity(compact[33:65]),
		V: compact[0],
	}, nil
}

// agentDigest returns the EIP-712 digest of Agent{source, connectionId}.
func agentDigest(source string, connectionID [32]byte) [32]byte {
	sourceHash := keccak256([]byte(source))
	structHash := keccak256(agentTypeHash[:], sourceHash[:], connectionID[:])
	domain := domainSeparator()
	return keccak256([]byte{0x19, 0x01}, domain[:], structHash[:])
}

// domainSeparator is the fixed EIP-712 domain hash for exchange actions.
func domainSeparator() [32]byte {
	nameHash := keccak256([]byte(signingDomainName))
	versionHash := keccak256([]byte(signingDomainVersion))
	chainID := leftPad32(big.NewInt(signingChainID).Bytes())
	verifyingContract := [32]byte{} // the zero address, ABI-encoded
	return keccak256(eip712DomainTypeHash[:], nameHash[:], versionHash[:], chainID[:], verifyingContract[:])
}

// leftPad32 left-pads a big-endian integer to an ABI word.
func leftPad32(value []byte) [32]byte {
	var word [32]byte
	copy(word[32-len(value):], value)
	return word
}

// hexQuantity renders big-endian bytes as a minimal-form 0x quantity, the
// form the venue's reference clients send.
func hexQuantity(value []byte) string {
	trimmed := strings.TrimLeft(hex.EncodeToString(value), "0")
	if trimmed == "" {
		return "0x0"
	}
	return "0x" + trimmed
}

// deriveAddress returns the lowercase 0x address of a public key: the last 20
// bytes of the keccak hash of its uncompressed encoding, minus the 0x04 tag.
func deriveAddress(publicKey *secp256k1.PublicKey) string {
	uncompressed := publicKey.SerializeUncompressed()
	digest := keccak256(uncompressed[1:])
	return "0x" + hex.EncodeToString(digest[12:])
}

// parseAddress decodes a 0x-prefixed EVM-style address.
func parseAddress(value string) ([]byte, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(value), "0x")
	decoded, err := hex.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: address %q is not valid hex: %w", value, err)
	}
	if len(decoded) != addressLen {
		return nil, fmt.Errorf("hyperliquid: address %q must be %d bytes, got %d", value, addressLen, len(decoded))
	}
	return decoded, nil
}
