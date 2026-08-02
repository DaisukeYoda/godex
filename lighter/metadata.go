package lighter

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// LoadExecutionMetadata resolves the market's execution metadata (size step,
// maintenance margin fraction) over public REST, without credentials. It
// serves consumers that pair real market data with a simulated executor
// (dry runs): the simulation quantizes exactly like the live executor would.
func LoadExecutionMetadata(ctx context.Context, cfg MarketDataConfig) (godex.ExecutionMetadata, error) {
	if cfg.MarketID < 0 {
		return godex.ExecutionMetadata{}, fmt.Errorf("lighter: MarketID must be non-negative")
	}
	baseURL := cfg.RESTBaseURL
	if baseURL == "" {
		if cfg.Network != Testnet && cfg.Network != Mainnet {
			return godex.ExecutionMetadata{}, fmt.Errorf("lighter: Network must be %q or %q, got %q", Testnet, Mainnet, cfg.Network)
		}
		baseURL, _ = cfg.Network.RESTBaseURL()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: restRequestTimeout}
	}
	response, err := getJSON[orderBookDetailsResponse](ctx, httpClient,
		strings.TrimSuffix(baseURL, "/"), "/api/v1/orderBookDetails")
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	for i := range *response.OrderBookDetails {
		detail := &(*response.OrderBookDetails)[i]
		if *detail.MarketID != cfg.MarketID {
			continue
		}
		if *detail.Status != marketStatusActive {
			return godex.ExecutionMetadata{}, fmt.Errorf("lighter: market %d is not active: %s", cfg.MarketID, *detail.Status)
		}
		return godex.ExecutionMetadata{
			SizeStep:                  decimal.New(1, *detail.SupportedSizeDecimals),
			MaintenanceMarginFraction: decimal.New(*detail.MaintenanceMarginFraction, marginFractionScale),
		}, nil
	}
	return godex.ExecutionMetadata{}, fmt.Errorf("lighter: market not found: market_id=%d", cfg.MarketID)
}
