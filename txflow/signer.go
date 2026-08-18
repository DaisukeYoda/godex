package txflow

// L1 action signing. An action is hashed into a "phantom agent" — an
// {txflowNetwork, chainId, apiVersion, connectionId} struct — which is then
// signed as EIP-712 typed data under a domain built from the same three
// network parameters. The verifying contract is the zero address: the domain
// is a protocol constant, not an EVM deployment.
//
// This is the Hyperliquid signing scheme with a wider Agent struct; the
// primitives live in internal/evmsign.

import (
	"fmt"
	"math/big"

	"github.com/DaisukeYoda/godex/internal/evmsign"
)

var agentTypeHash = evmsign.Keccak256([]byte(
	"Agent(string txflowNetwork,uint32 chainId,uint32 apiVersion,bytes32 connectionId)"))

// signature is the venue's signature envelope.
type signature = evmsign.Signature

// signer produces the signature for an exchange action. The interface covers
// the whole operation rather than the ECDSA primitive so tests can substitute
// it and inspect what reached it.
type signer interface {
	// signAction signs action for nonce.
	signAction(action any, nonce uint64) (signature, error)
	// address is the 0x address of the signing wallet.
	address() string
}

// keySigner signs with a raw secp256k1 key held in process. Use a TxFlow
// trading (agent) wallet approved for the account: it can trade but cannot
// withdraw, so a leaked trading process cannot move funds. The master key
// must never reach this process.
type keySigner struct {
	key    *evmsign.KeySigner
	params SigningParams
}

var _ signer = (*keySigner)(nil)

// newKeySigner parses a hex-encoded secp256k1 private key ("0x" prefix
// optional) and binds it to a network's signing parameters.
func newKeySigner(privateKeyHex string, params SigningParams) (*keySigner, error) {
	key, err := evmsign.NewKeySigner(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("txflow: %w", err)
	}
	if !params.complete() {
		return nil, fmt.Errorf("txflow: signing parameters are incomplete: %+v", params)
	}
	return &keySigner{key: key, params: params}, nil
}

func (s *keySigner) address() string { return s.key.Address() }

// signAction hashes the action into a connection id and signs the resulting
// Agent digest. Actions carry no vault marker: the venue's client sends no
// vault address, so the preimage always ends in the 0x00 "no vault" byte.
func (s *keySigner) signAction(action any, nonce uint64) (signature, error) {
	connectionID, err := evmsign.ActionHash(action, nil, nonce, nil)
	if err != nil {
		return signature{}, fmt.Errorf("txflow: %w", err)
	}
	sig, err := s.key.SignDigest(agentDigest(s.params, connectionID))
	if err != nil {
		return signature{}, fmt.Errorf("txflow: %w", err)
	}
	return sig, nil
}

// agentDigest returns the EIP-712 digest of
// Agent{txflowNetwork, chainId, apiVersion, connectionId} under the domain
// {name: txflowNetwork, version: apiVersion, chainId, verifyingContract: 0}.
func agentDigest(params SigningParams, connectionID [32]byte) [32]byte {
	networkHash := evmsign.Keccak256([]byte(params.Network))
	chainWord := evmsign.LeftPad32(big.NewInt(int64(params.ChainID)).Bytes())
	versionWord := evmsign.LeftPad32(big.NewInt(int64(params.APIVersion)).Bytes())
	structHash := evmsign.Keccak256(agentTypeHash[:], networkHash[:], chainWord[:], versionWord[:], connectionID[:])
	domain := evmsign.EIP712DomainSeparator(params.Network, fmt.Sprint(params.APIVersion),
		int64(params.ChainID), [evmsign.AddressLen]byte{})
	return evmsign.EIP712Digest(domain, structHash)
}
