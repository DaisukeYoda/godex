package lighter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
)

// Fixtures mirror the reference implementation's REST tests (omnibook
// lighter-connector.test.ts): fundings quote percent-per-hour with the sign in
// direction; orderBookDetails reports JSON floats.

// fakeMarketREST serves the two public market-data endpoints with settable
// bodies and records the fundings query parameters.
type fakeMarketREST struct {
	server *httptest.Server

	mu              sync.Mutex
	fundingsStatus  int
	fundingsBody    []byte
	detailsStatus   int
	detailsBody     []byte
	fundingsQueries []url.Values
}

func newFakeMarketREST(t *testing.T) *fakeMarketREST {
	t.Helper()
	rest := &fakeMarketREST{
		fundingsStatus: http.StatusOK,
		fundingsBody:   loadFixture(t, "market_fundings.json"),
		detailsStatus:  http.StatusOK,
		detailsBody:    loadFixture(t, "market_order_book_details.json"),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/fundings", func(w http.ResponseWriter, r *http.Request) {
		rest.mu.Lock()
		rest.fundingsQueries = append(rest.fundingsQueries, r.URL.Query())
		status, body := rest.fundingsStatus, rest.fundingsBody
		rest.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/api/v1/orderBookDetails", func(w http.ResponseWriter, _ *http.Request) {
		rest.mu.Lock()
		status, body := rest.detailsStatus, rest.detailsBody
		rest.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	rest.server = httptest.NewServer(mux)
	t.Cleanup(rest.server.Close)
	return rest
}

func (r *fakeMarketREST) setFundings(status int, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fundingsStatus = status
	r.fundingsBody = []byte(body)
}

func (r *fakeMarketREST) setDetails(status int, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.detailsStatus = status
	r.detailsBody = []byte(body)
}

func (r *fakeMarketREST) lastFundingsQuery(t *testing.T) url.Values {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fundingsQueries) == 0 {
		t.Fatal("no fundings request recorded")
	}
	return r.fundingsQueries[len(r.fundingsQueries)-1]
}

func newTestMarketData(t *testing.T, rest *fakeMarketREST, symbol godex.Symbol, marketID int64, mutate func(*MarketDataConfig)) *MarketData {
	t.Helper()
	cfg := MarketDataConfig{
		Symbol:      symbol,
		MarketID:    marketID,
		RESTBaseURL: rest.server.URL,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := NewMarketData(cfg)
	if err != nil {
		t.Fatalf("NewMarketData: %v", err)
	}
	return client
}

func TestFundingRateNormalization(t *testing.T) {
	tests := []struct {
		name     string
		symbol   godex.Symbol
		marketID int64
		body     string
		wantRate string
	}{
		{
			// Two entries: the latest timestamp (rate 0.0008) wins over the
			// older 0.0099. 0.0008%/h → /100 → 0.00000800 at scale 8.
			name:     "long pays folds positive and the latest timestamp is adopted",
			symbol:   "BTC-PERP",
			marketID: 1,
			body:     string(loadFixture(t, "market_fundings.json")),
			wantRate: "0.00000800",
		},
		{
			name:     "single long entry",
			symbol:   "ETH-PERP",
			marketID: 0,
			body:     `{"code":200,"fundings":[{"timestamp":1781193600,"rate":"0.0002","direction":"long"}]}`,
			wantRate: "0.00000200",
		},
		{
			name:     "short pays folds negative",
			symbol:   "SOL-PERP",
			marketID: 2,
			body:     `{"code":200,"fundings":[{"timestamp":1781193600,"rate":"0.0011","direction":"short"}]}`,
			wantRate: "-0.00001100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest := newFakeMarketREST(t)
			rest.setFundings(http.StatusOK, tt.body)
			client := newTestMarketData(t, rest, tt.symbol, tt.marketID, nil)
			rate, err := client.FundingRate(context.Background())
			if err != nil {
				t.Fatalf("FundingRate: %v", err)
			}
			if rate.VenueID != godex.VenueLighter || rate.Symbol != tt.symbol {
				t.Fatalf("unexpected identity: %s %s", rate.VenueID, rate.Symbol)
			}
			if got := rate.Rate.String(); got != tt.wantRate {
				t.Fatalf("rate = %s, want %s", got, tt.wantRate)
			}
			if rate.Rate.Scale() != godex.FundingRateScale {
				t.Fatalf("rate scale = %d, want %d", rate.Rate.Scale(), godex.FundingRateScale)
			}
			if rate.IntervalHours != fundingIntervalHours {
				t.Fatalf("interval = %d, want %d", rate.IntervalHours, fundingIntervalHours)
			}
			if rate.NextFundingTime != nil {
				t.Fatalf("next funding time = %v, want nil (not reported by the API)", rate.NextFundingTime)
			}
		})
	}
}

func TestFundingRateQueryParameters(t *testing.T) {
	rest := newFakeMarketREST(t)
	fixedNow := time.Unix(1781193600, 0)
	client := newTestMarketData(t, rest, "SOL-PERP", 2, func(cfg *MarketDataConfig) {
		cfg.Now = func() time.Time { return fixedNow }
	})
	if _, err := client.FundingRate(context.Background()); err != nil {
		t.Fatalf("FundingRate: %v", err)
	}
	query := rest.lastFundingsQuery(t)
	want := map[string]string{
		"market_id":       "2",
		"resolution":      fundingResolution,
		"count_back":      strconv.Itoa(fundingCountBack),
		"end_timestamp":   strconv.FormatInt(fixedNow.UnixMilli(), 10),
		"start_timestamp": strconv.FormatInt(fixedNow.Add(-fundingLookback).UnixMilli(), 10),
	}
	for key, value := range want {
		if got := query.Get(key); got != value {
			t.Errorf("query %s = %q, want %q", key, got, value)
		}
	}
}

func TestFundingRateFailures(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{
			name:    "no entries",
			status:  http.StatusOK,
			body:    `{"code":200,"fundings":[]}`,
			wantErr: "returned no entries",
		},
		{
			name:    "business error code",
			status:  http.StatusOK,
			body:    `{"code":20001,"message":"invalid param"}`,
			wantErr: "fundings response code 20001",
		},
		{
			name:    "http error",
			status:  http.StatusInternalServerError,
			body:    ``,
			wantErr: "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest := newFakeMarketREST(t)
			rest.setFundings(tt.status, tt.body)
			client := newTestMarketData(t, rest, "BTC-PERP", 1, nil)
			if _, err := client.FundingRate(context.Background()); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestMarketStatsConvertsFloatsThroughDecimalStrings(t *testing.T) {
	tests := []struct {
		name       string
		symbol     godex.Symbol
		marketID   int64
		wantOI     string
		wantVolume string
	}{
		// OI (base units) × last_trade_price, rounded once at the product;
		// volume is already USD and rounds to cents.
		{"btc", "BTC-PERP", 1, "105431340.10", "719934279.91"},
		{"eth", "ETH-PERP", 0, "57553650.00", "350632845.42"},
		{"sol", "SOL-PERP", 2, "7860000.00", "51074544.22"},
	}
	rest := newFakeMarketREST(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestMarketData(t, rest, tt.symbol, tt.marketID, nil)
			stats, err := client.MarketStats(context.Background())
			if err != nil {
				t.Fatalf("MarketStats: %v", err)
			}
			if stats.VenueID != godex.VenueLighter || stats.Symbol != tt.symbol {
				t.Fatalf("unexpected identity: %s %s", stats.VenueID, stats.Symbol)
			}
			if got := stats.OpenInterestUSD.String(); got != tt.wantOI {
				t.Fatalf("open interest = %s, want %s", got, tt.wantOI)
			}
			if got := stats.Volume24hUSD.String(); got != tt.wantVolume {
				t.Fatalf("volume = %s, want %s", got, tt.wantVolume)
			}
		})
	}
}

func TestMarketStatsFailures(t *testing.T) {
	tests := []struct {
		name     string
		marketID int64
		status   int
		body     string
		wantErr  string
	}{
		{
			name:     "configured market absent",
			marketID: 99,
			status:   http.StatusOK,
			body:     "", // fixture default
			wantErr:  "market 99 not found in orderBookDetails",
		},
		{
			name:     "entry without market_id is not a match",
			marketID: 1,
			status:   http.StatusOK,
			body:     `{"code":200,"order_book_details":[{"last_trade_price":62000.2,"daily_quote_token_volume":1.0,"open_interest":1.0}]}`,
			wantErr:  "market 1 not found in orderBookDetails",
		},
		{
			name:     "matched entry missing a stat field",
			marketID: 1,
			status:   http.StatusOK,
			body:     `{"code":200,"order_book_details":[{"market_id":1,"daily_quote_token_volume":1.0,"open_interest":1.0}]}`,
			wantErr:  `missing required field "last_trade_price"`,
		},
		{
			name:     "business error code",
			marketID: 1,
			status:   http.StatusOK,
			body:     `{"code":20001,"message":"invalid param"}`,
			wantErr:  "orderBookDetails response code 20001",
		},
		{
			name:     "http error",
			marketID: 1,
			status:   http.StatusInternalServerError,
			body:     ``,
			wantErr:  "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest := newFakeMarketREST(t)
			if tt.body != "" || tt.status != http.StatusOK {
				rest.setDetails(tt.status, tt.body)
			}
			client := newTestMarketData(t, rest, "BTC-PERP", tt.marketID, nil)
			if _, err := client.MarketStats(context.Background()); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewMarketDataConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MarketDataConfig
		wantErr string
	}{
		{
			name:    "missing symbol",
			cfg:     MarketDataConfig{MarketID: 1, RESTBaseURL: "http://venue.invalid"},
			wantErr: "Symbol is required",
		},
		{
			name:    "negative market id",
			cfg:     MarketDataConfig{Symbol: "BTC-PERP", MarketID: -1, RESTBaseURL: "http://venue.invalid"},
			wantErr: "MarketID must be non-negative",
		},
		{
			name:    "no url and no network",
			cfg:     MarketDataConfig{Symbol: "BTC-PERP", MarketID: 1},
			wantErr: "Network must be",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMarketData(tt.cfg); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
