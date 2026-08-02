package lighter

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
)

// percentDivisor converts the venue's percent-per-hour funding quotes to
// plain decimal rates.
var percentDivisor = decimal.MustFromString("100", 0)

// MarketDataConfig parameterizes a lighter MarketData client.
type MarketDataConfig struct {
	// Symbol is the normalized label stamped on results (e.g. "SOL-PERP").
	Symbol godex.Symbol
	// MarketID is the venue's numeric market identifier.
	MarketID int64
	Network  Network

	// Test/ops overrides. Zero values resolve from Network.
	RESTBaseURL string
	HTTPClient  *http.Client
	Now         func() time.Time
}

// MarketData polls the public REST API for funding and market statistics of
// one market. Safe for concurrent use.
type MarketData struct {
	symbol     godex.Symbol
	marketID   int64
	baseURL    string
	httpClient *http.Client
	now        func() time.Time
}

var _ godex.MarketDataClient = (*MarketData)(nil)

// NewMarketData builds a MarketData client. It does not touch the network
// until the first query.
func NewMarketData(cfg MarketDataConfig) (*MarketData, error) {
	if cfg.Symbol == "" {
		return nil, fmt.Errorf("lighter: Symbol is required")
	}
	if cfg.MarketID < 0 {
		return nil, fmt.Errorf("lighter: MarketID must be non-negative")
	}
	baseURL := cfg.RESTBaseURL
	if baseURL == "" {
		if cfg.Network != Testnet && cfg.Network != Mainnet {
			return nil, fmt.Errorf("lighter: Network must be %q or %q, got %q", Testnet, Mainnet, cfg.Network)
		}
		baseURL, _ = cfg.Network.RESTBaseURL()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: restRequestTimeout}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &MarketData{
		symbol:     cfg.Symbol,
		marketID:   cfg.MarketID,
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: httpClient,
		now:        now,
	}, nil
}

// VenueID implements godex.MarketDataClient.
func (m *MarketData) VenueID() godex.VenueID { return godex.VenueLighter }

// FundingRate implements godex.MarketDataClient. The venue reports settled
// hourly fundings as percent-per-hour with the sign carried in direction;
// the latest settled entry is adopted, the direction folded into the sign
// (long pays = positive), and the percent quote divided down to a plain rate
// at godex.FundingRateScale. Funding applies at the top of the hour but the
// API does not report the next time directly, so NextFundingTime is nil.
func (m *MarketData) FundingRate(ctx context.Context) (godex.FundingRate, error) {
	end := m.now()
	query := url.Values{
		"market_id":       {strconv.FormatInt(m.marketID, 10)},
		"resolution":      {fundingResolution},
		"start_timestamp": {strconv.FormatInt(end.Add(-fundingLookback).UnixMilli(), 10)},
		"end_timestamp":   {strconv.FormatInt(end.UnixMilli(), 10)},
		"count_back":      {strconv.Itoa(fundingCountBack)},
	}
	response, err := getJSON[fundingsResponse](ctx, m.httpClient, m.baseURL, "/api/v1/fundings?"+query.Encode())
	if err != nil {
		return godex.FundingRate{}, err
	}
	if len(response.Fundings) == 0 {
		return godex.FundingRate{}, fmt.Errorf("lighter: fundings for market %d returned no entries", m.marketID)
	}
	latest := response.Fundings[0]
	for _, entry := range response.Fundings[1:] {
		if *entry.Timestamp >= *latest.Timestamp {
			latest = entry
		}
	}
	percent, err := decimal.FromDecimalString(*latest.Rate)
	if err != nil {
		return godex.FundingRate{}, fmt.Errorf("lighter: fundings rate: %w", err)
	}
	if *latest.Direction == fundingDirectionShort {
		percent = percent.Neg()
	}
	return godex.FundingRate{
		VenueID:       godex.VenueLighter,
		Symbol:        m.symbol,
		Rate:          percent.DivToScale(percentDivisor, godex.FundingRateScale),
		IntervalHours: fundingIntervalHours,
	}, nil
}

// MarketStats implements godex.MarketDataClient. Uniquely for this venue the
// endpoint reports JSON floats; they are formatted back to decimal strings at
// the ingestion boundary. open_interest is in base-asset units and is
// converted with the venue's last trade price, rounding once at the product;
// daily_quote_token_volume is already USD-denominated.
func (m *MarketData) MarketStats(ctx context.Context) (godex.MarketStats, error) {
	response, err := getJSON[statsBookDetailsResponse](ctx, m.httpClient, m.baseURL, "/api/v1/orderBookDetails")
	if err != nil {
		return godex.MarketStats{}, err
	}
	detail, err := response.detail(m.marketID)
	if err != nil {
		return godex.MarketStats{}, err
	}
	openInterest, err := decimal.FromDecimalString(statString(*detail.OpenInterest))
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("lighter: orderBookDetails open_interest: %w", err)
	}
	lastPrice, err := decimal.FromDecimalString(statString(*detail.LastTradePrice))
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("lighter: orderBookDetails last_trade_price: %w", err)
	}
	volume, err := decimal.FromStringRounded(statString(*detail.DailyQuoteTokenVolume), godex.USDNotionalScale)
	if err != nil {
		return godex.MarketStats{}, fmt.Errorf("lighter: orderBookDetails daily_quote_token_volume: %w", err)
	}
	return godex.MarketStats{
		VenueID:         godex.VenueLighter,
		Symbol:          m.symbol,
		OpenInterestUSD: openInterest.MulToScale(lastPrice, godex.USDNotionalScale),
		Volume24hUSD:    volume,
	}, nil
}

// statString formats a JSON float back into a plain decimal string for the
// strict decimal ingestion boundary. 'f' formatting never produces exponent
// notation, and -1 keeps the shortest round-trip digits.
func statString(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
