package dydx

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// MarketDataConfig parameterizes a dydx MarketData client.
type MarketDataConfig struct {
	// Symbol is the normalized label stamped on results (e.g. "SOL-PERP").
	Symbol godex.Symbol
	// Ticker is the venue market ticker (e.g. "SOL-USD").
	Ticker  string
	Network Network

	// Test/ops overrides. Zero values resolve from Network.
	IndexerRESTBaseURL string
	HTTPClient         *http.Client
}

// MarketData polls the Indexer REST API for funding and market statistics of
// one market. Safe for concurrent use.
type MarketData struct {
	symbol     godex.Symbol
	ticker     string
	baseURL    string
	httpClient *http.Client
}

var _ godex.MarketDataClient = (*MarketData)(nil)

// NewMarketData builds a MarketData client. It does not touch the network
// until the first query.
func NewMarketData(cfg MarketDataConfig) (*MarketData, error) {
	if cfg.Symbol == "" {
		return nil, fmt.Errorf("dydx: Symbol is required")
	}
	if cfg.Ticker == "" {
		return nil, fmt.Errorf("dydx: Ticker is required (e.g. \"SOL-USD\")")
	}
	baseURL := cfg.IndexerRESTBaseURL
	if baseURL == "" {
		if cfg.Network != Testnet && cfg.Network != Mainnet {
			return nil, fmt.Errorf("dydx: Network must be %q or %q, got %q", Testnet, Mainnet, cfg.Network)
		}
		baseURL, _ = cfg.Network.IndexerRESTBaseURL()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: restRequestTimeout}
	}
	return &MarketData{
		symbol:     cfg.Symbol,
		ticker:     cfg.Ticker,
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: httpClient,
	}, nil
}

// VenueID implements godex.MarketDataClient.
func (m *MarketData) VenueID() godex.VenueID { return godex.VenueDydx }

// FundingRate implements godex.MarketDataClient. nextFundingRate is the
// venue's native 1-hour rate at unpredictable precision, rounded explicitly
// to godex.FundingRateScale. The Indexer does not report the next funding
// time, so NextFundingTime is nil.
func (m *MarketData) FundingRate(ctx context.Context) (godex.FundingRate, error) {
	market, err := m.fetchMarket(ctx)
	if err != nil {
		return godex.FundingRate{}, err
	}
	rate, err := decimal.FromStringRounded(*market.NextFundingRate, godex.FundingRateScale)
	if err != nil {
		return godex.FundingRate{}, fmt.Errorf("dydx: market %q nextFundingRate: %w", m.ticker, err)
	}
	return godex.FundingRate{
		VenueID:       godex.VenueDydx,
		Symbol:        m.symbol,
		Rate:          rate,
		IntervalHours: fundingIntervalHours,
	}, nil
}

// MarketStats implements godex.MarketDataClient. openInterest is in
// base-asset units and is converted with the venue's oracle price, rounding
// once at the product; volume24H is already USD-denominated.
func (m *MarketData) MarketStats(ctx context.Context) (godex.MarketStats, error) {
	market, err := m.fetchMarket(ctx)
	if err != nil {
		return godex.MarketStats{}, err
	}
	openInterest, err := decimal.FromDecimalString(*market.OpenInterest)
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("dydx: market %q openInterest: %w", m.ticker, err)
	}
	oraclePrice, err := decimal.FromDecimalString(*market.OraclePrice)
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("dydx: market %q oraclePrice: %w", m.ticker, err)
	}
	volume, err := decimal.FromStringRounded(*market.Volume24H, godex.USDNotionalScale)
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("dydx: market %q volume24H: %w", m.ticker, err)
	}
	return godex.MarketStats{
		VenueID:         godex.VenueDydx,
		Symbol:          m.symbol,
		OpenInterestUSD: openInterest.MulToScale(oraclePrice, godex.USDNotionalScale),
		Volume24hUSD:    volume,
	}, nil
}

func (m *MarketData) fetchMarket(ctx context.Context) (*marketStatsMarket, error) {
	response, err := getJSON[marketStatsResponse](ctx, m.httpClient, m.baseURL, "/perpetualMarkets")
	if err != nil {
		return nil, err
	}
	return response.market(m.ticker)
}
