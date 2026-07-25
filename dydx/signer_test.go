package dydx

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	authpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/auth/v1beta1"
	signingpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/tx/signing/v1beta1"
	txpb "github.com/DaisukeYoda/godex/dydx/internal/pb/cosmos/tx/v1beta1"
	accountpluspb "github.com/DaisukeYoda/godex/dydx/internal/pb/dydxprotocol/accountplus"
	clobpb "github.com/DaisukeYoda/godex/dydx/internal/pb/dydxprotocol/clob"
)

// testPrivateKeyHex is a throwaway key used only by these offline tests; its
// address is pinned in address_test.go.
const testPrivateKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

const testAddress = "dydx1nduq8yy8h4nr7g9vuuglzklqatmaquq9zm99js"

func newTestSigner(t *testing.T) *keySigner {
	t.Helper()
	signer, err := newKeySigner(testPrivateKeyHex)
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	return signer
}

func testEnvelope() txParams {
	return txParams{
		chainID:       testnetChainID,
		accountNumber: 4242,
		sequence:      7,
	}
}

func testPlaceOrderParams() placeOrderParams {
	return placeOrderParams{
		orderIdentity: orderIdentity{
			address:          testAddress,
			subaccountNumber: 0,
			clientID:         123456789,
			clobPairID:       1,
		},
		side:         sideBuy,
		quantums:     1_000_000,
		subticks:     2_500_000_000,
		goodTilBlock: 987_654,
		timeInForce:  timeInForcePostOnly,
	}
}

// decodeSignedTx unpacks a broadcast payload back into its parts.
func decodeSignedTx(t *testing.T, txBytes []byte) (*txpb.TxRaw, *txpb.TxBody, *txpb.AuthInfo) {
	t.Helper()
	var raw txpb.TxRaw
	if err := proto.Unmarshal(txBytes, &raw); err != nil {
		t.Fatalf("decode TxRaw: %v", err)
	}
	var body txpb.TxBody
	if err := proto.Unmarshal(raw.GetBodyBytes(), &body); err != nil {
		t.Fatalf("decode TxBody: %v", err)
	}
	var authInfo txpb.AuthInfo
	if err := proto.Unmarshal(raw.GetAuthInfoBytes(), &authInfo); err != nil {
		t.Fatalf("decode AuthInfo: %v", err)
	}
	return &raw, &body, &authInfo
}

// TestSignPlaceOrderProducesVerifiableSignature checks that the signature in
// the broadcast payload verifies against the SignDoc rebuilt from that same
// payload — the property the chain itself checks.
func TestSignPlaceOrderProducesVerifiableSignature(t *testing.T) {
	signer := newTestSigner(t)
	envelope := testEnvelope()

	txBytes, err := signer.signPlaceOrder(testPlaceOrderParams(), envelope)
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	raw, _, _ := decodeSignedTx(t, txBytes)

	if len(raw.GetSignatures()) != 1 {
		t.Fatalf("got %d signatures, want 1", len(raw.GetSignatures()))
	}
	signature := raw.GetSignatures()[0]
	if len(signature) != signatureLen {
		t.Fatalf("signature is %d bytes, want %d (raw R||S, never DER)", len(signature), signatureLen)
	}

	digest, err := signDocDigest(raw.GetBodyBytes(), raw.GetAuthInfoBytes(),
		envelope.chainID, envelope.accountNumber)
	if err != nil {
		t.Fatalf("signDocDigest: %v", err)
	}
	var r, s secp256k1.ModNScalar
	r.SetByteSlice(signature[:32])
	s.SetByteSlice(signature[32:])
	if !ecdsa.NewSignature(&r, &s).Verify(digest, signer.privateKey.PubKey()) {
		t.Fatal("signature does not verify against the SignDoc rebuilt from the broadcast bytes")
	}
}

// TestSignatureIsLowS guards the Cosmos canonical-signature requirement: S must
// not exceed half the group order. A high-S signature verifies fine offline but
// is rejected on chain.
func TestSignatureIsLowS(t *testing.T) {
	signer := newTestSigner(t)
	halfOrder := new(big.Int).Rsh(secp256k1.Params().N, 1)

	// Sign several distinct orders; roughly half of unnormalized signatures
	// would land above the halfway point, so a handful makes a regression
	// overwhelmingly likely to show up.
	for clientID := uint32(1); clientID <= 8; clientID++ {
		params := testPlaceOrderParams()
		params.clientID = clientID
		txBytes, err := signer.signPlaceOrder(params, testEnvelope())
		if err != nil {
			t.Fatalf("signPlaceOrder: %v", err)
		}
		raw, _, _ := decodeSignedTx(t, txBytes)
		s := new(big.Int).SetBytes(raw.GetSignatures()[0][32:])
		if s.Cmp(halfOrder) > 0 {
			t.Fatalf("clientID %d: S is above half the group order (not canonical low-S)", clientID)
		}
	}
}

// TestSignPlaceOrderEncodesShortTermOrder pins the order fields that actually
// reach the chain.
func TestSignPlaceOrderEncodesShortTermOrder(t *testing.T) {
	signer := newTestSigner(t)
	params := testPlaceOrderParams()

	txBytes, err := signer.signPlaceOrder(params, testEnvelope())
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	_, body, authInfo := decodeSignedTx(t, txBytes)

	if len(body.GetMessages()) != 1 {
		t.Fatalf("got %d messages, want 1", len(body.GetMessages()))
	}
	if got := body.GetMessages()[0].GetTypeUrl(); got != msgPlaceOrderTypeURL {
		t.Fatalf("type URL = %q, want %q", got, msgPlaceOrderTypeURL)
	}
	var message clobpb.MsgPlaceOrder
	if err := proto.Unmarshal(body.GetMessages()[0].GetValue(), &message); err != nil {
		t.Fatalf("decode MsgPlaceOrder: %v", err)
	}
	order := message.GetOrder()
	if order.GetOrderId().GetOrderFlags() != orderFlagsShortTerm {
		t.Fatalf("order flags = %d, want %d (short-term)",
			order.GetOrderId().GetOrderFlags(), orderFlagsShortTerm)
	}
	if order.GetOrderId().GetClientId() != params.clientID {
		t.Fatalf("client id = %d, want %d", order.GetOrderId().GetClientId(), params.clientID)
	}
	if order.GetOrderId().GetClobPairId() != params.clobPairID {
		t.Fatalf("clob pair id = %d, want %d", order.GetOrderId().GetClobPairId(), params.clobPairID)
	}
	if got := order.GetOrderId().GetSubaccountId().GetOwner(); got != testAddress {
		t.Fatalf("owner = %q, want %q", got, testAddress)
	}
	if order.GetSide() != clobpb.Side_SIDE_BUY {
		t.Fatalf("side = %v, want SIDE_BUY", order.GetSide())
	}
	if order.GetQuantums() != params.quantums || order.GetSubticks() != params.subticks {
		t.Fatalf("quantums/subticks = %d/%d, want %d/%d",
			order.GetQuantums(), order.GetSubticks(), params.quantums, params.subticks)
	}
	if order.GetGoodTilBlock() != params.goodTilBlock {
		t.Fatalf("good_til_block = %d, want %d", order.GetGoodTilBlock(), params.goodTilBlock)
	}
	if order.GetGoodTilBlockTime() != 0 {
		t.Fatal("good_til_block_time must stay unset for short-term orders")
	}
	if order.GetTimeInForce() != clobpb.TimeInForce_TIME_IN_FORCE_POST_ONLY {
		t.Fatalf("time_in_force = %v, want POST_ONLY", order.GetTimeInForce())
	}

	// Short-term orders are fee-exempt: gas limit set, no fee coins.
	if got := authInfo.GetFee().GetGasLimit(); got != gasLimit {
		t.Fatalf("gas limit = %d, want %d", got, gasLimit)
	}
	if len(authInfo.GetFee().GetAmount()) != 0 {
		t.Fatal("short-term orders must carry no fee coins")
	}
	signerInfo := authInfo.GetSignerInfos()[0]
	if signerInfo.GetSequence() != testEnvelope().sequence {
		t.Fatalf("sequence = %d, want %d", signerInfo.GetSequence(), testEnvelope().sequence)
	}
	if got := signerInfo.GetModeInfo().GetSingle().GetMode(); got != signingpb.SignMode_SIGN_MODE_DIRECT {
		t.Fatalf("sign mode = %v, want SIGN_MODE_DIRECT", got)
	}
	if got := signerInfo.GetPublicKey().GetTypeUrl(); got != pubKeyTypeURL {
		t.Fatalf("public key type URL = %q, want %q", got, pubKeyTypeURL)
	}
}

func TestSignCancelOrderEncodesShortTermCancel(t *testing.T) {
	signer := newTestSigner(t)
	params := cancelOrderParams{
		orderIdentity: testPlaceOrderParams().orderIdentity,
		goodTilBlock:  987_700,
	}

	txBytes, err := signer.signCancelOrder(params, testEnvelope())
	if err != nil {
		t.Fatalf("signCancelOrder: %v", err)
	}
	_, body, _ := decodeSignedTx(t, txBytes)

	if got := body.GetMessages()[0].GetTypeUrl(); got != msgCancelOrderTypeURL {
		t.Fatalf("type URL = %q, want %q", got, msgCancelOrderTypeURL)
	}
	var message clobpb.MsgCancelOrder
	if err := proto.Unmarshal(body.GetMessages()[0].GetValue(), &message); err != nil {
		t.Fatalf("decode MsgCancelOrder: %v", err)
	}
	if message.GetOrderId().GetClientId() != params.clientID {
		t.Fatalf("client id = %d, want %d", message.GetOrderId().GetClientId(), params.clientID)
	}
	if message.GetGoodTilBlock() != params.goodTilBlock {
		t.Fatalf("good_til_block = %d, want %d", message.GetGoodTilBlock(), params.goodTilBlock)
	}
}

// TestSignPlaceOrderAttachesAuthenticator covers the scoped trading key path.
//
// selected_authenticators is positional — the chain rejects a transaction whose
// authenticator count differs from its message count ("Mismatch between the
// number of selected authenticators and messages"). Every transaction here
// carries one message, so the list must hold exactly one id.
func TestSignPlaceOrderAttachesAuthenticator(t *testing.T) {
	signer := newTestSigner(t)
	envelope := testEnvelope()
	authenticatorID := uint64(11)
	envelope.authenticatorID = &authenticatorID

	txBytes, err := signer.signPlaceOrder(testPlaceOrderParams(), envelope)
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	_, body, _ := decodeSignedTx(t, txBytes)

	options := body.GetNonCriticalExtensionOptions()
	if len(options) != 1 {
		t.Fatalf("got %d extension options, want 1", len(options))
	}
	if options[0].GetTypeUrl() != txExtensionTypeURL {
		t.Fatalf("extension type URL = %q, want %q", options[0].GetTypeUrl(), txExtensionTypeURL)
	}
	var extension accountpluspb.TxExtension
	if err := proto.Unmarshal(options[0].GetValue(), &extension); err != nil {
		t.Fatalf("decode TxExtension: %v", err)
	}
	selected := extension.GetSelectedAuthenticators()
	if len(selected) != len(body.GetMessages()) {
		t.Fatalf("got %d selected authenticators for %d messages; the chain requires one per message",
			len(selected), len(body.GetMessages()))
	}
	if selected[0] != authenticatorID {
		t.Fatalf("selected authenticator = %d, want %d", selected[0], authenticatorID)
	}
}

// TestSignPlaceOrderOmitsExtensionWithoutAuthenticators keeps the default path
// byte-identical to a plain single-key transaction.
func TestSignPlaceOrderOmitsExtensionWithoutAuthenticators(t *testing.T) {
	signer := newTestSigner(t)
	txBytes, err := signer.signPlaceOrder(testPlaceOrderParams(), testEnvelope())
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	_, body, _ := decodeSignedTx(t, txBytes)
	if len(body.GetNonCriticalExtensionOptions()) != 0 {
		t.Fatal("expected no extension options when no authenticators are configured")
	}
}

// TestSignIsDeterministic documents that identical inputs produce identical
// bytes (RFC 6979), so a resubmission of the same order cannot present two
// different transaction hashes.
func TestSignIsDeterministic(t *testing.T) {
	signer := newTestSigner(t)
	first, err := signer.signPlaceOrder(testPlaceOrderParams(), testEnvelope())
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	second, err := signer.signPlaceOrder(testPlaceOrderParams(), testEnvelope())
	if err != nil {
		t.Fatalf("signPlaceOrder: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("signing the same order twice produced different bytes")
	}
}

func TestNewKeySignerDerivesAddress(t *testing.T) {
	signer := newTestSigner(t)
	if signer.address() != testAddress {
		t.Fatalf("address = %q, want %q", signer.address(), testAddress)
	}
	if len(signer.pubKey()) != compressedPubKeyLen {
		t.Fatalf("public key is %d bytes, want %d", len(signer.pubKey()), compressedPubKeyLen)
	}
}

func TestNewKeySignerAcceptsHexPrefix(t *testing.T) {
	signer, err := newKeySigner("0x" + strings.ToUpper(testPrivateKeyHex))
	if err != nil {
		t.Fatalf("newKeySigner: %v", err)
	}
	if signer.address() != testAddress {
		t.Fatalf("address = %q, want %q", signer.address(), testAddress)
	}
}

func TestNewKeySignerRejectsBadKeys(t *testing.T) {
	for _, testCase := range []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "not hex", key: "zz0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"},
		{name: "too short", key: "4c0883a6"},
		{name: "zero scalar", key: strings.Repeat("0", 64)},
		{
			name: "at group order",
			key:  "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newKeySigner(testCase.key); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestBuildAndSignTxRejectsWrongSignatureLength(t *testing.T) {
	envelope := testEnvelope()
	envelope.pubKeyCompressed = newTestSigner(t).pubKey()
	_, err := buildAndSignTx(msgPlaceOrderTypeURL, buildPlaceOrderMessage(testPlaceOrderParams()),
		envelope, func([]byte) ([]byte, error) { return make([]byte, 65), nil })
	if err == nil {
		t.Fatal("expected an error for a 65-byte (recoverable) signature")
	}
}

func TestBuildAndSignTxRequiresChainID(t *testing.T) {
	envelope := testEnvelope()
	envelope.chainID = ""
	envelope.pubKeyCompressed = newTestSigner(t).pubKey()
	_, err := buildAndSignTx(msgPlaceOrderTypeURL, buildPlaceOrderMessage(testPlaceOrderParams()),
		envelope, func([]byte) ([]byte, error) { return make([]byte, signatureLen), nil })
	if err == nil {
		t.Fatal("expected an error when the chain id is missing")
	}
}

func TestDecodeBaseAccount(t *testing.T) {
	baseAccount := mustMarshalBaseAccount(t, 4242, 7)
	response, err := marshal(&authpb.QueryAccountResponse{Account: baseAccount})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	accountNumber, sequence, err := decodeBaseAccount(response)
	if err != nil {
		t.Fatalf("decodeBaseAccount: %v", err)
	}
	if accountNumber != 4242 || sequence != 7 {
		t.Fatalf("got account number %d sequence %d, want 4242/7", accountNumber, sequence)
	}
}

func TestDecodeBaseAccountRejectsUnexpectedAccountType(t *testing.T) {
	baseAccount := mustMarshalBaseAccount(t, 1, 1)
	baseAccount.TypeUrl = "/cosmos.auth.v1beta1.ModuleAccount"
	response, err := marshal(&authpb.QueryAccountResponse{Account: baseAccount})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	if _, _, err := decodeBaseAccount(response); err == nil {
		t.Fatal("expected an error for a non-BaseAccount account type")
	}
}

func TestDecodeBaseAccountRejectsMissingAccount(t *testing.T) {
	response, err := marshal(&authpb.QueryAccountResponse{})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	if _, _, err := decodeBaseAccount(response); err == nil {
		t.Fatal("expected an error when the query returns no account")
	}
}

func TestEncodeAccountQueryRoundTrips(t *testing.T) {
	payload, err := encodeAccountQuery(testAddress)
	if err != nil {
		t.Fatalf("encodeAccountQuery: %v", err)
	}
	var request authpb.QueryAccountRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		t.Fatalf("decode QueryAccountRequest: %v", err)
	}
	if request.GetAddress() != testAddress {
		t.Fatalf("address = %q, want %q", request.GetAddress(), testAddress)
	}
}

func mustMarshalBaseAccount(t *testing.T, accountNumber, sequence uint64) *anypb.Any {
	t.Helper()
	value, err := marshal(&authpb.BaseAccount{
		Address:       testAddress,
		AccountNumber: accountNumber,
		Sequence:      sequence,
	})
	if err != nil {
		t.Fatalf("marshal BaseAccount: %v", err)
	}
	return &anypb.Any{TypeUrl: baseAccountTypeURL, Value: value}
}
