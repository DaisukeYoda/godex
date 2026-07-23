package lighter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const sendTxPath = "/api/v1/sendTx"

// getJSON fetches a REST path and decodes it into a validated wire type.
func getJSON[T any, PT interface {
	*T
	validate() error
}](ctx context.Context, client *http.Client, baseURL, path string) (PT, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("lighter: REST %s request: %w", path, err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("lighter: REST %s failed: %w", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lighter: REST %s failed: HTTP %d", path, response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("lighter: REST %s read failed: %w", path, err)
	}
	value := PT(new(T))
	if err := json.Unmarshal(body, value); err != nil {
		return nil, fmt.Errorf("lighter: REST %s returned malformed JSON: %w", path, err)
	}
	if err := value.validate(); err != nil {
		return nil, fmt.Errorf("lighter: REST %s: %w", path, err)
	}
	return value, nil
}

// sendTx submits a signed transaction. Outcome classification:
//   - ("", nil): the venue accepted the transaction (body code 200).
//   - (message, nil): a known API rejection — the venue processed and
//     refused it; the server nonce did not advance.
//   - ("", err): the outcome is unknown (transport failure, timeout,
//     unparseable body); the caller must latch a fault, never blind-retry.
func sendTx(ctx context.Context, client *http.Client, baseURL string, txType uint8, txInfo string) (string, error) {
	form := url.Values{
		"tx_type": {strconv.Itoa(int(txType))},
		"tx_info": {txInfo},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+sendTxPath, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("lighter: sendTx request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("lighter: sendTx failed: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("lighter: sendTx response read failed: %w", err)
	}
	var parsed sendTxResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("lighter: sendTx returned malformed JSON: %w", err)
	}
	if err := parsed.validate(); err != nil {
		return "", err
	}
	if *parsed.Code == restSuccessCode {
		return "", nil
	}
	if parsed.Message != nil {
		return *parsed.Message, nil
	}
	return fmt.Sprintf("sendTx failed with code %d", *parsed.Code), nil
}
