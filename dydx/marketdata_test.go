package dydx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DaisukeYoda/godex"
)

// statsMarketJSON builds one perpetualMarkets entry with the stats fields.
func statsMarketJSON(oraclePrice, nextFundingRate, openInterest, volume24H string) string {
	return fmt.Sprintf(`{"oraclePrice":"%s","nextFundingRate":"%s","openInterest":"%s","volume24H":"%s"}`,
		oraclePrice, nextFundingRate, openInterest, volume24H)
}

func newStatsServer(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/perpetualMarkets" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestMarketData(t *testing.T, server *httptest.Server) *MarketData {
	t.Helper()
	client, err := NewMarketData(MarketDataConfig{
		Symbol:             "SOL-PERP",
		Ticker:             "SOL-USD",
		IndexerRESTBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewMarketData: %v", err)
	}
	return client
}

func TestFundingRateRoundsNativePrecision(t *testing.T) {
	cases := []struct {
		name   string
		native string
		want   string
	}{
		// The venue quotes ~20 fractional digits; rounding is half away from
		// zero to FundingRateScale (8).
		{"long native precision", "-0.00000078846153846154", "-0.00000079"},
		{"positive rounds up at the midpoint", "0.000000015", "0.00000002"},
		{"plain zero", "0", "0.00000000"},
		{"short precision widens", "0.0001", "0.00010000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"markets":{"SOL-USD":%s}}`,
				statsMarketJSON("128.5", tc.native, "1000", "500000"))
			client := newTestMarketData(t, newStatsServer(t, body, http.StatusOK))
			rate, err := client.FundingRate(context.Background())
			if err != nil {
				t.Fatalf("FundingRate: %v", err)
			}
			if rate.VenueID != godex.VenueDydx || rate.Symbol != "SOL-PERP" {
				t.Fatalf("identity = %s %s", rate.VenueID, rate.Symbol)
			}
			if rate.IntervalHours != 1 {
				t.Fatalf("interval = %d, want 1", rate.IntervalHours)
			}
			if rate.NextFundingTime != nil {
				t.Fatal("the Indexer does not report a next funding time")
			}
			if got := rate.Rate.String(); got != tc.want {
				t.Fatalf("rate = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestMarketStatsConvertsOpenInterestOnce(t *testing.T) {
	// openInterest is base-asset units: 1234.567 SOL * $128.5 = $158641.8595,
	// rounded once at the product to cents (half away from zero -> .86).
	body := fmt.Sprintf(`{"markets":{"SOL-USD":%s}}`,
		statsMarketJSON("128.5", "0", "1234.567", "9876543.219"))
	client := newTestMarketData(t, newStatsServer(t, body, http.StatusOK))
	stats, err := client.MarketStats(context.Background())
	if err != nil {
		t.Fatalf("MarketStats: %v", err)
	}
	if got := stats.OpenInterestUSD.String(); got != "158641.86" {
		t.Fatalf("openInterestUsd = %s, want 158641.86", got)
	}
	if got := stats.Volume24hUSD.String(); got != "9876543.22" {
		t.Fatalf("volume24hUsd = %s, want 9876543.22", got)
	}
}

func TestMarketDataFailures(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		status  int
		wantErr string
	}{
		{
			name:    "configured market absent",
			body:    fmt.Sprintf(`{"markets":{"ETH-USD":%s}}`, statsMarketJSON("1", "0", "1", "1")),
			status:  http.StatusOK,
			wantErr: `market "SOL-USD" is not listed`,
		},
		{
			name:    "entry missing a stat field",
			body:    `{"markets":{"SOL-USD":{"oraclePrice":"128.5"}}}`,
			status:  http.StatusOK,
			wantErr: "missing required field",
		},
		{
			name:    "missing markets container",
			body:    `{}`,
			status:  http.StatusOK,
			wantErr: `missing required field "markets"`,
		},
		{
			name:    "http error",
			body:    "oops",
			status:  http.StatusBadGateway,
			wantErr: "HTTP 502",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestMarketData(t, newStatsServer(t, tc.body, tc.status))
			if _, err := client.FundingRate(context.Background()); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestNewMarketDataConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  MarketDataConfig
	}{
		{"missing symbol", MarketDataConfig{Ticker: "SOL-USD", IndexerRESTBaseURL: "http://example.invalid"}},
		{"missing ticker", MarketDataConfig{Symbol: "SOL-PERP", IndexerRESTBaseURL: "http://example.invalid"}},
		{"no url and no network", MarketDataConfig{Symbol: "SOL-PERP", Ticker: "SOL-USD"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewMarketData(tc.cfg); err == nil {
				t.Fatal("expected a config error")
			}
		})
	}
}

func fundingPaymentJSON(createdAt, side, size, rate, payment string) string {
	return fmt.Sprintf(`{"createdAt":"%s","ticker":"SOL-USD","side":"%s","size":"%s","rate":"%s","payment":"%s"}`,
		createdAt, side, size, rate, payment)
}

func TestFetchFundingPayments(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fundingPayments" {
			http.NotFound(w, r)
			return
		}
		gotQuery = r.URL.RawQuery
		_, _ = fmt.Fprintf(w, `{"fundingPayments":[%s,%s]}`,
			fundingPaymentJSON("2026-07-31T08:00:00.000Z", "LONG", "8", "0.0000125", "0.007331"),
			fundingPaymentJSON("2026-07-31T07:00:00.000Z", "SHORT", "-8", "0.0000125", "-0.007331"))
	}))
	t.Cleanup(server.Close)

	payments, err := FetchFundingPayments(context.Background(), FundingPaymentsConfig{
		Address:            "dydx1test",
		SubaccountNumber:   0,
		Limit:              48,
		IndexerRESTBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("FetchFundingPayments: %v", err)
	}
	if want := "address=dydx1test&limit=48&subaccountNumber=0"; gotQuery != want {
		t.Fatalf("query = %s, want %s", gotQuery, want)
	}
	if len(payments) != 2 {
		t.Fatalf("payments = %d, want 2", len(payments))
	}
	first := payments[0]
	if first.CreatedAt.UTC().Format("2006-01-02T15:04:05") != "2026-07-31T08:00:00" {
		t.Fatalf("createdAt = %s", first.CreatedAt)
	}
	if first.Side != "LONG" || first.Ticker != "SOL-USD" {
		t.Fatalf("identity = %s %s", first.Side, first.Ticker)
	}
	if got := first.Payment.String(); got != "0.007331" {
		t.Fatalf("payment = %s", got)
	}
	if got := payments[1].Payment.String(); got != "-0.007331" {
		t.Fatalf("negative payment = %s", got)
	}
}

func TestFetchFundingPaymentsFailures(t *testing.T) {
	badSide := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"fundingPayments":[%s]}`,
			fundingPaymentJSON("2026-07-31T08:00:00.000Z", "SIDEWAYS", "8", "0", "0"))
	}))
	t.Cleanup(badSide.Close)
	if _, err := FetchFundingPayments(context.Background(), FundingPaymentsConfig{
		Address: "dydx1test", Limit: 1, IndexerRESTBaseURL: badSide.URL,
	}); err == nil || !strings.Contains(err.Error(), `unknown side "SIDEWAYS"`) {
		t.Fatalf("err = %v, want unknown side", err)
	}

	if _, err := FetchFundingPayments(context.Background(), FundingPaymentsConfig{
		Address: "dydx1test", Limit: 0, IndexerRESTBaseURL: badSide.URL,
	}); err == nil || !strings.Contains(err.Error(), "Limit must be positive") {
		t.Fatalf("err = %v, want limit error", err)
	}

	if _, err := FetchFundingPayments(context.Background(), FundingPaymentsConfig{
		Limit: 1, IndexerRESTBaseURL: badSide.URL,
	}); err == nil || !strings.Contains(err.Error(), "Address is required") {
		t.Fatalf("err = %v, want address error", err)
	}
}
