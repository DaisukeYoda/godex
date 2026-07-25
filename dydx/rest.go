package dydx

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// HTTP access to the two hosts the adapter talks to: the Indexer's REST API for
// market and account state, and the validator's CometBFT JSON-RPC for block
// height, account lookups, and transaction broadcast.

// getJSON fetches a REST path and decodes it into a validated wire type.
func getJSON[T any, PT interface {
	*T
	validate() error
}](ctx context.Context, client *http.Client, baseURL, path string) (PT, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("dydx: GET %s request: %w", path, err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("dydx: GET %s failed: %w", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dydx: GET %s failed: HTTP %d", path, response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("dydx: GET %s read failed: %w", path, err)
	}
	value := PT(new(T))
	if err := json.Unmarshal(body, value); err != nil {
		return nil, fmt.Errorf("dydx: GET %s returned malformed JSON: %w", path, err)
	}
	if err := value.validate(); err != nil {
		return nil, fmt.Errorf("dydx: GET %s: %w", path, err)
	}
	return value, nil
}

// fetchMarkets loads the venue's market metadata.
func fetchMarkets(ctx context.Context, client *http.Client, indexerBaseURL string) (*perpetualMarketsResponse, error) {
	return getJSON[perpetualMarketsResponse](ctx, client, indexerBaseURL, "/perpetualMarkets")
}

// fetchSubaccount loads the account snapshot used to converge local state.
func fetchSubaccount(
	ctx context.Context,
	client *http.Client,
	indexerBaseURL, address string,
	subaccountNumber uint32,
) (*subaccountResponse, error) {
	path := fmt.Sprintf("/addresses/%s/subaccountNumber/%d", url.PathEscape(address), subaccountNumber)
	return getJSON[subaccountResponse](ctx, client, indexerBaseURL, path)
}

// fetchFills loads fills newest first. createdBeforeOrAt, when non-empty, pages
// further back in time from a previous page's oldest fill.
func fetchFills(
	ctx context.Context,
	client *http.Client,
	indexerBaseURL, address string,
	subaccountNumber uint32,
	limit int,
	createdBeforeOrAt string,
) (*fillsResponse, error) {
	query := url.Values{
		"address":          {address},
		"subaccountNumber": {strconv.FormatUint(uint64(subaccountNumber), 10)},
		"limit":            {strconv.Itoa(limit)},
	}
	if createdBeforeOrAt != "" {
		query.Set("createdBeforeOrAt", createdBeforeOrAt)
	}
	return getJSON[fillsResponse](ctx, client, indexerBaseURL, "/fills?"+query.Encode())
}

// fetchOpenOrders lists the subaccount's resting orders. Used to reconcile the
// local order table after an unknown submission outcome.
func fetchOpenOrders(
	ctx context.Context,
	client *http.Client,
	indexerBaseURL, address string,
	subaccountNumber uint32,
) (*ordersListResponse, error) {
	query := url.Values{
		"address":          {address},
		"subaccountNumber": {strconv.FormatUint(uint64(subaccountNumber), 10)},
		"status":           {orderStatusOpen},
	}
	return getJSON[ordersListResponse](ctx, client, indexerBaseURL, "/orders?"+query.Encode())
}

// fetchHeight reads the chain tip from the validator's CometBFT RPC. The
// Indexer's own height lags the chain and must not be used for good_til_block.
func fetchHeight(ctx context.Context, client *http.Client, rpcBaseURL string) (uint32, error) {
	status, err := getJSON[cometStatusResponse](ctx, client, rpcBaseURL, "/status")
	if err != nil {
		return 0, err
	}
	return status.height()
}

// jsonRPCRequest is a CometBFT JSON-RPC call.
type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// postRPC issues a CometBFT JSON-RPC call and decodes it into a validated wire
// type. Note that validation failure covers both malformed responses and
// venue-reported errors; callers that must distinguish an accepted transaction
// from a rejected one inspect the decoded body instead.
func postRPC[T any, PT interface {
	*T
	validate() error
}](ctx context.Context, client *http.Client, rpcBaseURL, method string, params any) (PT, []byte, error) {
	payload, err := json.Marshal(jsonRPCRequest{JSONRPC: "2.0", ID: -1, Method: method, Params: params})
	if err != nil {
		return nil, nil, fmt.Errorf("dydx: encode %s request: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcBaseURL+"/", bytes.NewReader(payload))
	if err != nil {
		return nil, nil, fmt.Errorf("dydx: %s request: %w", method, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, fmt.Errorf("dydx: %s failed: %w", method, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("dydx: %s response read failed: %w", method, err)
	}
	// CometBFT answers HTTP 500 for JSON-RPC-level errors while still returning
	// a usable body, so the status code alone does not decide the outcome.
	value := PT(new(T))
	if err := json.Unmarshal(body, value); err != nil {
		return nil, body, fmt.Errorf("dydx: %s returned malformed JSON (HTTP %d): %w",
			method, response.StatusCode, err)
	}
	if err := value.validate(); err != nil {
		return nil, body, err
	}
	return value, body, nil
}

// fetchAccountInfo reads an address's account number and sequence through
// abci_query, so the adapter needs no Cosmos LCD host.
func fetchAccountInfo(
	ctx context.Context,
	client *http.Client,
	rpcBaseURL, address string,
) (accountNumber, sequence uint64, err error) {
	query, err := encodeAccountQuery(address)
	if err != nil {
		return 0, 0, err
	}
	response, _, err := postRPC[abciQueryResponse](ctx, client, rpcBaseURL, "abci_query", map[string]any{
		"path":   accountQueryPath,
		"data":   hex.EncodeToString(query),
		"height": "0",
		"prove":  false,
	})
	if err != nil {
		return 0, 0, err
	}
	value, err := base64.StdEncoding.DecodeString(*response.Result.Response.Value)
	if err != nil {
		return 0, 0, fmt.Errorf("dydx: abci_query value is not base64: %w", err)
	}
	return decodeBaseAccount(value)
}

// broadcastOutcome classifies a broadcast_tx_sync response.
type broadcastOutcome int

const (
	// broadcastAccepted means the transaction entered the mempool. It does not
	// mean the order filled.
	broadcastAccepted broadcastOutcome = iota
	// broadcastPostOnlyCrossed is the synchronous rejection of a post-only
	// order that would have taken liquidity — a normal-path outcome.
	broadcastPostOnlyCrossed
	// broadcastRejected is any other definite venue rejection. The transaction
	// was processed and refused; it had no effect.
	broadcastRejected
)

// broadcastResult is a classified broadcast_tx_sync response.
type broadcastResult struct {
	outcome broadcastOutcome
	code    int
	log     string
	hash    string
}

// broadcastTx submits a signed transaction. The error return is reserved for
// genuinely unknown outcomes — a transport failure, timeout, or unparseable
// body — which the caller must latch as a fault rather than retry, because the
// transaction may or may not have reached the mempool. Every definite venue
// answer, including a rejection, comes back as a nil error with a classified
// result.
func broadcastTx(ctx context.Context, client *http.Client, rpcBaseURL string, txBytes []byte) (broadcastResult, error) {
	response, _, err := postRPC[broadcastTxResponse](ctx, client, rpcBaseURL, "broadcast_tx_sync", map[string]any{
		"tx": base64.StdEncoding.EncodeToString(txBytes),
	})
	if err != nil {
		return broadcastResult{}, err
	}
	result := broadcastResult{code: *response.Result.Code}
	if response.Result.Log != nil {
		result.log = *response.Result.Log
	}
	if response.Result.Hash != nil {
		result.hash = *response.Result.Hash
	}
	switch {
	case result.code == txCodeOK:
		result.outcome = broadcastAccepted
	case result.code == clobErrPostOnlyWouldCross && codespaceIs(response, clobCodespace):
		result.outcome = broadcastPostOnlyCrossed
	default:
		result.outcome = broadcastRejected
	}
	return result, nil
}

func codespaceIs(response *broadcastTxResponse, codespace string) bool {
	return response.Result.Codespace != nil && *response.Result.Codespace == codespace
}
