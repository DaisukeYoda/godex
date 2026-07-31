package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// maxResponseBytes bounds a REST body so a misbehaving endpoint cannot
// exhaust memory. Account snapshots and meta responses are far smaller.
const maxResponseBytes = 8 << 20

// postJSON posts an /info request and decodes the reply into a validated wire
// type.
func postJSON[T any, PT interface {
	*T
	validate() error
}](ctx context.Context, client *http.Client, baseURL string, request any) (PT, error) {
	body, err := doPost(ctx, client, baseURL+infoPath, request)
	if err != nil {
		return nil, err
	}
	value := PT(new(T))
	if err := json.Unmarshal(body, value); err != nil {
		return nil, fmt.Errorf("hyperliquid: /info returned malformed JSON: %w", err)
	}
	if err := value.validate(); err != nil {
		return nil, err
	}
	return value, nil
}

// doPost performs a JSON POST and returns the body, requiring HTTP 200. A
// non-200 status is reported as an error without interpretation: on /info it
// is a failed read, and on /exchange the caller treats it as an unknown
// outcome rather than guessing whether the action was applied.
func doPost(ctx context.Context, client *http.Client, url string, payload any) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: encoding request for %s failed: %w", url, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: %s request: %w", url, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: %s failed: %w", url, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: %s read failed: %w", url, err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hyperliquid: %s failed: HTTP %d: %s", url, response.StatusCode, truncate(body))
	}
	return body, nil
}

func truncate(body []byte) string {
	const limit = 256
	if len(body) > limit {
		return string(body[:limit]) + "…"
	}
	return string(body)
}

// infoRequest is an /info query. dex is omitted so the request targets the
// first-party perp dex.
type infoRequest struct {
	Type string `json:"type"`
	User string `json:"user,omitempty"`
	// Oid identifies a single order for an orderStatus query. It carries a
	// client order id here — the id assigned before submission, which is the
	// only handle an ambiguous submission leaves behind.
	Oid string `json:"oid,omitempty"`
	// Coin scopes a per-asset query such as activeAssetData.
	Coin string `json:"coin,omitempty"`
}

// exchangeRequest is a signed action submission.
type exchangeRequest struct {
	Action       any       `json:"action"`
	Nonce        uint64    `json:"nonce"`
	Signature    signature `json:"signature"`
	VaultAddress string    `json:"vaultAddress,omitempty"`
}

// postExchange submits a signed action. Outcome classification mirrors the
// contract's ambiguity rule:
//   - (statuses, "", nil): the venue processed the submission; per-order
//     outcomes are in statuses.
//   - (nil, message, nil): the venue processed and refused the whole
//     submission (status "err") — nothing was applied.
//   - (nil, "", err): the outcome is unknown (transport failure, timeout,
//     non-200, unparseable body). The caller must latch a fault and
//     reconcile; it must never blind-retry.
//
// A non-200 is deliberately unknown rather than "definitely not applied": the
// venue does not promise that a rejected HTTP status implies an unapplied
// action, and reconciliation is cheap while a double-submitted order is not.
func postExchange(ctx context.Context, client *http.Client, baseURL string, request exchangeRequest) ([]json.RawMessage, string, error) {
	body, err := doPost(ctx, client, baseURL+exchangePath, request)
	if err != nil {
		return nil, "", err
	}
	var parsed exchangeResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", fmt.Errorf("hyperliquid: /exchange returned malformed JSON: %w", err)
	}
	if err := parsed.validate(); err != nil {
		return nil, "", err
	}
	if *parsed.Status != statusOK {
		var message string
		if err := json.Unmarshal(parsed.Response, &message); err != nil {
			message = string(parsed.Response)
		}
		return nil, fmt.Sprintf("%s: %s", *parsed.Status, message), nil
	}

	var success exchangeSuccess
	if err := json.Unmarshal(parsed.Response, &success); err != nil {
		return nil, "", fmt.Errorf("hyperliquid: /exchange response body is malformed: %w", err)
	}
	if err := success.validate(); err != nil {
		return nil, "", err
	}
	if success.Data == nil || success.Data.Statuses == nil {
		return nil, "", fmt.Errorf("hyperliquid: /exchange %q response carries no statuses", *success.Type)
	}
	return *success.Data.Statuses, "", nil
}
