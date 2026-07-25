package dydx

import (
	"crypto/sha256"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	authpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/auth/v1beta1"
	secp256k1pb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/crypto/secp256k1"
	signingpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/tx/signing/v1beta1"
	txpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/tx/v1beta1"
	accountpluspb "github.com/DaisukeYoda/godex/dydx/internal/pb/dydxprotocol/accountplus"
	clobpb "github.com/DaisukeYoda/godex/dydx/internal/pb/dydxprotocol/clob"
	subaccountspb "github.com/DaisukeYoda/godex/dydx/internal/pb/dydxprotocol/subaccounts"
)

// Cosmos transaction assembly. Everything here is pure: no I/O, no clock, no
// key material. The signature itself is supplied by a callback so the encoding
// can be tested independently of the signer.
//
// A SIGN_MODE_DIRECT signature commits to the exact serialized body and auth
// info bytes, and those same bytes are what gets broadcast — so each is
// marshaled once and threaded through both the SignDoc and the TxRaw. Round
// tripping through the message structs instead would risk signing one encoding
// and sending another.

// orderIdentity is the venue's identifier for an order: the subaccount that
// owns it, the caller-assigned client id, and the market.
type orderIdentity struct {
	address          string
	subaccountNumber uint32
	clientID         uint32
	clobPairID       uint32
}

// placeOrderParams is a fully resolved short-term order, in wire units.
type placeOrderParams struct {
	orderIdentity
	side         int32
	quantums     uint64
	subticks     uint64
	goodTilBlock uint32
	timeInForce  int32
	reduceOnly   bool
}

// cancelOrderParams cancels a short-term order. goodTilBlock bounds how long
// the cancellation itself stays valid; it need not match the original order's.
type cancelOrderParams struct {
	orderIdentity
	goodTilBlock uint32
}

// txParams is the envelope context shared by every transaction.
type txParams struct {
	chainID          string
	accountNumber    uint64
	sequence         uint64
	pubKeyCompressed []byte
	// authenticatorID, when set, is the scoped authenticator authorizing this
	// transaction's single message.
	authenticatorID *uint64
}

func (o orderIdentity) toProto() *clobpb.OrderId {
	return &clobpb.OrderId{
		SubaccountId: &subaccountspb.SubaccountId{
			Owner:  o.address,
			Number: o.subaccountNumber,
		},
		ClientId:   o.clientID,
		OrderFlags: orderFlagsShortTerm,
		ClobPairId: o.clobPairID,
	}
}

// buildPlaceOrderMessage builds the MsgPlaceOrder for a short-term order.
func buildPlaceOrderMessage(params placeOrderParams) *clobpb.MsgPlaceOrder {
	return &clobpb.MsgPlaceOrder{
		Order: &clobpb.Order{
			OrderId:      params.toProto(),
			Side:         clobpb.Side(params.side),
			Quantums:     params.quantums,
			Subticks:     params.subticks,
			GoodTilOneof: &clobpb.Order_GoodTilBlock{GoodTilBlock: params.goodTilBlock},
			TimeInForce:  clobpb.TimeInForce(params.timeInForce),
			ReduceOnly:   params.reduceOnly,
		},
	}
}

// buildCancelOrderMessage builds the MsgCancelOrder for a short-term order.
func buildCancelOrderMessage(params cancelOrderParams) *clobpb.MsgCancelOrder {
	return &clobpb.MsgCancelOrder{
		OrderId:      params.toProto(),
		GoodTilOneof: &clobpb.MsgCancelOrder_GoodTilBlock{GoodTilBlock: params.goodTilBlock},
	}
}

// marshal serializes a message deterministically. Determinism is not required
// for validity — the chain verifies the bytes it receives — but it keeps test
// expectations stable.
func marshal(message proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

// toAny wraps a message in an Any under the given type URL. The chain resolves
// messages by type URL, so it must be spelled exactly as the chain registers it.
func toAny(typeURL string, message proto.Message) (*anypb.Any, error) {
	value, err := marshal(message)
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal %s: %w", typeURL, err)
	}
	return &anypb.Any{TypeUrl: typeURL, Value: value}, nil
}

// buildTxBody wraps a single message into a TxBody, attaching the authenticator
// extension when a scoped trading key is in use.
//
// selected_authenticators is positional: the chain requires exactly one id per
// message and rejects any other count. This body always carries one message, so
// the extension carries exactly one id.
func buildTxBody(typeURL string, message proto.Message, authenticatorID *uint64) (*txpb.TxBody, error) {
	messageAny, err := toAny(typeURL, message)
	if err != nil {
		return nil, err
	}
	body := &txpb.TxBody{Messages: []*anypb.Any{messageAny}}
	if authenticatorID != nil {
		extension, err := toAny(txExtensionTypeURL,
			&accountpluspb.TxExtension{SelectedAuthenticators: []uint64{*authenticatorID}})
		if err != nil {
			return nil, err
		}
		body.NonCriticalExtensionOptions = []*anypb.Any{extension}
	}
	return body, nil
}

// buildAuthInfo describes the single signer and the fee. Short-term orders are
// fee-exempt, so the coin list is empty and only the gas limit is set.
func buildAuthInfo(pubKeyCompressed []byte, sequence uint64) (*txpb.AuthInfo, error) {
	if len(pubKeyCompressed) != compressedPubKeyLen {
		return nil, fmt.Errorf("dydx: compressed public key must be %d bytes, got %d",
			compressedPubKeyLen, len(pubKeyCompressed))
	}
	pubKeyAny, err := toAny(pubKeyTypeURL, &secp256k1pb.PubKey{Key: pubKeyCompressed})
	if err != nil {
		return nil, err
	}
	return &txpb.AuthInfo{
		SignerInfos: []*txpb.SignerInfo{{
			PublicKey: pubKeyAny,
			ModeInfo: &txpb.ModeInfo{
				Sum: &txpb.ModeInfo_Single_{
					Single: &txpb.ModeInfo_Single{Mode: signingpb.SignMode_SIGN_MODE_DIRECT},
				},
			},
			Sequence: sequence,
		}},
		Fee: &txpb.Fee{GasLimit: gasLimit},
	}, nil
}

// signDocDigest returns the SHA-256 digest a SIGN_MODE_DIRECT signature is
// computed over.
func signDocDigest(bodyBytes, authInfoBytes []byte, chainID string, accountNumber uint64) ([]byte, error) {
	signDocBytes, err := marshal(&txpb.SignDoc{
		BodyBytes:     bodyBytes,
		AuthInfoBytes: authInfoBytes,
		ChainId:       chainID,
		AccountNumber: accountNumber,
	})
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal SignDoc: %w", err)
	}
	digest := sha256.Sum256(signDocBytes)
	return digest[:], nil
}

// buildAndSignTx assembles, signs, and encodes a transaction carrying exactly
// one message. sign receives the SignDoc digest and must return a raw 64-byte
// low-S secp256k1 signature.
func buildAndSignTx(
	typeURL string,
	message proto.Message,
	params txParams,
	sign func(digest []byte) ([]byte, error),
) ([]byte, error) {
	if params.chainID == "" {
		return nil, fmt.Errorf("dydx: chain id is required to sign")
	}
	body, err := buildTxBody(typeURL, message, params.authenticatorID)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := marshal(body)
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal TxBody: %w", err)
	}
	authInfo, err := buildAuthInfo(params.pubKeyCompressed, params.sequence)
	if err != nil {
		return nil, err
	}
	authInfoBytes, err := marshal(authInfo)
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal AuthInfo: %w", err)
	}

	digest, err := signDocDigest(bodyBytes, authInfoBytes, params.chainID, params.accountNumber)
	if err != nil {
		return nil, err
	}
	signature, err := sign(digest)
	if err != nil {
		return nil, fmt.Errorf("dydx: sign: %w", err)
	}
	if len(signature) != signatureLen {
		return nil, fmt.Errorf("dydx: signature must be %d bytes, got %d", signatureLen, len(signature))
	}

	txBytes, err := marshal(&txpb.TxRaw{
		BodyBytes:     bodyBytes,
		AuthInfoBytes: authInfoBytes,
		Signatures:    [][]byte{signature},
	})
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal TxRaw: %w", err)
	}
	return txBytes, nil
}

// decodeBaseAccount extracts the account number and sequence from an
// abci_query response value for /cosmos.auth.v1beta1.Query/Account.
func decodeBaseAccount(value []byte) (accountNumber, sequence uint64, err error) {
	var response authpb.QueryAccountResponse
	if err := proto.Unmarshal(value, &response); err != nil {
		return 0, 0, fmt.Errorf("dydx: decode QueryAccountResponse: %w", err)
	}
	account := response.GetAccount()
	if account == nil {
		return 0, 0, fmt.Errorf("dydx: account query returned no account")
	}
	// Trading addresses are plain BaseAccounts. Anything else (a module or
	// vesting account) is not something godex should be signing for.
	if account.GetTypeUrl() != baseAccountTypeURL {
		return 0, 0, fmt.Errorf("dydx: unsupported account type %q, want %q",
			account.GetTypeUrl(), baseAccountTypeURL)
	}
	var baseAccount authpb.BaseAccount
	if err := proto.Unmarshal(account.GetValue(), &baseAccount); err != nil {
		return 0, 0, fmt.Errorf("dydx: decode BaseAccount: %w", err)
	}
	return baseAccount.GetAccountNumber(), baseAccount.GetSequence(), nil
}

// encodeAccountQuery builds the abci_query request payload for an address's
// account.
func encodeAccountQuery(address string) ([]byte, error) {
	payload, err := marshal(&authpb.QueryAccountRequest{Address: address})
	if err != nil {
		return nil, fmt.Errorf("dydx: marshal QueryAccountRequest: %w", err)
	}
	return payload, nil
}
