package dydx

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/DaisukeYoda/godex"
)

// LoadExecutionMetadata resolves the market's execution metadata (size step,
// maintenance margin fraction) over public REST, without credentials. It
// serves consumers that pair real market data with a simulated executor
// (dry runs): the simulation quantizes exactly like the live executor would.
func LoadExecutionMetadata(ctx context.Context, cfg MarketDataConfig) (godex.ExecutionMetadata, error) {
	if cfg.Ticker == "" {
		return godex.ExecutionMetadata{}, fmt.Errorf("dydx: Ticker is required (e.g. \"SOL-USD\")")
	}
	baseURL := cfg.IndexerRESTBaseURL
	if baseURL == "" {
		if cfg.Network != Testnet && cfg.Network != Mainnet {
			return godex.ExecutionMetadata{}, fmt.Errorf("dydx: Network must be %q or %q, got %q", Testnet, Mainnet, cfg.Network)
		}
		baseURL, _ = cfg.Network.IndexerRESTBaseURL()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: restRequestTimeout}
	}
	response, err := fetchMarkets(ctx, httpClient, strings.TrimSuffix(baseURL, "/"))
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	market, err := response.market(cfg.Ticker)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	meta, err := newMarketMeta(market)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	return godex.ExecutionMetadata{
		SizeStep:                  meta.step,
		MaintenanceMarginFraction: meta.maintenanceMarginFraction,
	}, nil
}
