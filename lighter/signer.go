package lighter

import (
	"fmt"
	"time"

	lighterclient "github.com/elliottech/lighter-go/client"
	lighterhttp "github.com/elliottech/lighter-go/client/http"
	lightertypes "github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
)

// signer abstracts transaction signing so tests can substitute a fake. The
// production implementation wraps the official lighter-go TxClient — the same
// code the venue ships as its c-shared signer library, so signatures match
// the reference implementation by construction.
type signer interface {
	// signCreateOrder returns the sendTx (tx_type, tx_info) pair for a
	// signed order-creation transaction.
	signCreateOrder(params createOrderParams) (txType uint8, txInfo string, err error)
	// signCancelOrder cancels by client order index (cancel-by-client-index
	// semantics verified on testnet).
	signCancelOrder(marketIndex int16, clientOrderIndex int64, nonce int64) (txType uint8, txInfo string, err error)
	// createAuthToken mints an account-WS subscription token.
	createAuthToken(deadline time.Time) (string, error)
	// check validates that the private key matches the API key registered on
	// the server (fail fast at connect).
	check() error
}

type createOrderParams struct {
	marketIndex      int16
	clientOrderIndex int64
	// baseAmount and price are wire integers at the market's supported
	// decimals; the executor bounds-checks them before signing.
	baseAmount int64
	price      uint32
	isAsk      bool
	postOnly   bool // false = IOC; the contract supports no other TIF
	reduceOnly bool
	// orderExpiryAt is epoch ms for post-only (GTT) orders and
	// txtypes.NilOrderExpiry for IOC (the venue requires exactly that).
	orderExpiryAt int64
	nonce         int64
}

// txSigner is the lighter-go-backed signer.
type txSigner struct {
	client *lighterclient.TxClient
}

func newTxSigner(restBaseURL, apiPrivateKey string, accountIndex int64, apiKeyIndex uint8, chainID uint32) (*txSigner, error) {
	// The HTTP client is used by Check (API key lookup) only: the executor
	// always passes explicit nonces, so the SDK never fetches its own.
	apiClient := lighterhttp.NewClient(restBaseURL)
	if apiClient == nil {
		return nil, fmt.Errorf("lighter: signer REST base URL must not be empty")
	}
	client, err := lighterclient.NewTxClient(apiClient, apiPrivateKey, accountIndex, apiKeyIndex, chainID)
	if err != nil {
		return nil, fmt.Errorf("lighter: signer construction failed: %w", err)
	}
	return &txSigner{client: client}, nil
}

func boolToWire(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func (s *txSigner) signCreateOrder(params createOrderParams) (uint8, string, error) {
	timeInForce := uint8(txtypes.ImmediateOrCancel)
	if params.postOnly {
		timeInForce = txtypes.PostOnly
	}
	request := &lightertypes.CreateOrderTxReq{
		MarketIndex:      params.marketIndex,
		ClientOrderIndex: params.clientOrderIndex,
		BaseAmount:       params.baseAmount,
		Price:            params.price,
		IsAsk:            boolToWire(params.isAsk),
		Type:             txtypes.LimitOrder,
		TimeInForce:      timeInForce,
		ReduceOnly:       boolToWire(params.reduceOnly),
		TriggerPrice:     txtypes.NilOrderTriggerPrice,
		OrderExpiry:      params.orderExpiryAt,
	}
	nonce := params.nonce
	// TransactOpts.ExpiredAt stays zero: the SDK fills the default tx
	// deadline (10 min − 1 s), the same path the shared-lib signer uses.
	info, err := s.client.GetCreateOrderTransaction(request, &lightertypes.TransactOpts{Nonce: &nonce})
	if err != nil {
		return 0, "", fmt.Errorf("lighter: sign create order: %w", err)
	}
	txInfo, err := info.GetTxInfo()
	if err != nil {
		return 0, "", fmt.Errorf("lighter: encode create order tx: %w", err)
	}
	return info.GetTxType(), txInfo, nil
}

func (s *txSigner) signCancelOrder(marketIndex int16, clientOrderIndex int64, nonce int64) (uint8, string, error) {
	request := &lightertypes.CancelOrderTxReq{
		MarketIndex: marketIndex,
		Index:       clientOrderIndex,
	}
	info, err := s.client.GetCancelOrderTransaction(request, &lightertypes.TransactOpts{Nonce: &nonce})
	if err != nil {
		return 0, "", fmt.Errorf("lighter: sign cancel order: %w", err)
	}
	txInfo, err := info.GetTxInfo()
	if err != nil {
		return 0, "", fmt.Errorf("lighter: encode cancel order tx: %w", err)
	}
	return info.GetTxType(), txInfo, nil
}

func (s *txSigner) createAuthToken(deadline time.Time) (string, error) {
	token, err := s.client.GetAuthToken(deadline)
	if err != nil {
		return "", fmt.Errorf("lighter: create auth token: %w", err)
	}
	return token, nil
}

func (s *txSigner) check() error {
	if err := s.client.Check(); err != nil {
		return fmt.Errorf("lighter: api key validation failed: %w", err)
	}
	return nil
}
