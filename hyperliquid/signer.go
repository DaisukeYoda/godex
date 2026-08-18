package hyperliquid

// L1 action signing. An action is hashed into a "phantom agent" — a
// {source, connectionId} struct — which is then signed as EIP-712 typed data
// under a fixed domain. The domain is not an EVM deployment: chain id 1337
// and the zero verifying contract are literal protocol constants, and the
// mainnet/testnet split lives in the agent's source field instead.

import (
	"fmt"

	"github.com/DaisukeYoda/godex/internal/evmsign"
)

var agentTypeHash = evmsign.Keccak256([]byte("Agent(string source,bytes32 connectionId)"))

// signature is the venue's signature envelope.
type signature = evmsign.Signature

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
	key          *evmsign.KeySigner
	source       string
	vaultAddress []byte
}

var _ signer = (*keySigner)(nil)

// newKeySigner parses a hex-encoded secp256k1 private key ("0x" prefix
// optional) and binds it to a network source and optional vault address.
func newKeySigner(privateKeyHex, source string, vaultAddress []byte) (*keySigner, error) {
	key, err := evmsign.NewKeySigner(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: %w", err)
	}
	if length := len(vaultAddress); length != 0 && length != evmsign.AddressLen {
		return nil, fmt.Errorf("hyperliquid: vault address must be %d bytes, got %d", evmsign.AddressLen, length)
	}
	return &keySigner{key: key, source: source, vaultAddress: vaultAddress}, nil
}

func (s *keySigner) address() string { return s.key.Address() }

func (s *keySigner) signAction(action any, nonce uint64) (signature, error) {
	connectionID, err := actionHash(action, s.vaultAddress, nonce, nil)
	if err != nil {
		return signature{}, err
	}
	sig, err := s.key.SignDigest(agentDigest(s.source, connectionID))
	if err != nil {
		return signature{}, fmt.Errorf("hyperliquid: %w", err)
	}
	return sig, nil
}

// actionHash builds the connection id for an action; see evmsign.ActionHash.
func actionHash(action any, vaultAddress []byte, nonce uint64, expiresAfter *uint64) ([32]byte, error) {
	hash, err := evmsign.ActionHash(action, vaultAddress, nonce, expiresAfter)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hyperliquid: %w", err)
	}
	return hash, nil
}

// agentDigest returns the EIP-712 digest of Agent{source, connectionId}.
func agentDigest(source string, connectionID [32]byte) [32]byte {
	sourceHash := evmsign.Keccak256([]byte(source))
	structHash := evmsign.Keccak256(agentTypeHash[:], sourceHash[:], connectionID[:])
	domain := evmsign.EIP712DomainSeparator(signingDomainName, signingDomainVersion, signingChainID, [evmsign.AddressLen]byte{})
	return evmsign.EIP712Digest(domain, structHash)
}

// parseAddress decodes a 0x-prefixed EVM-style address.
func parseAddress(value string) ([]byte, error) {
	decoded, err := evmsign.ParseAddress(value)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: %w", err)
	}
	return decoded, nil
}
