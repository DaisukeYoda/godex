package dydx

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/DaisukeYoda/godex/decimal"
)

// FundingPayment is one settled funding payment from the Indexer's
// per-account history (GET /v4/fundingPayments). Unlike the account
// snapshot's netFunding — a per-position running total that resets when the
// position closes — payments arrive one per funding interval, so any window
// can be aggregated from them.
type FundingPayment struct {
	CreatedAt time.Time
	Ticker    string
	// Side is the position side the payment applied to ("LONG"/"SHORT"); the
	// payment's sign lives in Payment.
	Side string
	// Size is the position size the payment applied to, in base-asset units.
	Size decimal.Decimal
	// Rate is the funding rate applied.
	Rate decimal.Decimal
	// Payment is the settled USD amount: positive received, negative paid.
	Payment decimal.Decimal
}

// FundingPaymentsConfig parameterizes FetchFundingPayments. This is public
// Indexer data — no credentials involved.
type FundingPaymentsConfig struct {
	// Address is the bech32 account ("dydx1...").
	Address string
	// SubaccountNumber selects the subaccount (0 is the default one).
	SubaccountNumber uint32
	// Limit caps the number of newest-first records returned (one per funding
	// interval per open position).
	Limit   int
	Network Network

	// Test/ops overrides. Zero values resolve from Network.
	IndexerRESTBaseURL string
	HTTPClient         *http.Client
}

// FetchFundingPayments loads an account's settled funding payments, newest
// first.
func FetchFundingPayments(ctx context.Context, cfg FundingPaymentsConfig) ([]FundingPayment, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("dydx: Address is required")
	}
	if cfg.Limit <= 0 {
		return nil, fmt.Errorf("dydx: Limit must be positive")
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

	query := url.Values{
		"address":          {cfg.Address},
		"subaccountNumber": {strconv.FormatUint(uint64(cfg.SubaccountNumber), 10)},
		"limit":            {strconv.Itoa(cfg.Limit)},
	}
	response, err := getJSON[fundingPaymentsResponse](ctx, httpClient,
		strings.TrimSuffix(baseURL, "/"), "/fundingPayments?"+query.Encode())
	if err != nil {
		return nil, err
	}

	payments := make([]FundingPayment, 0, len(*response.FundingPayments))
	for i := range *response.FundingPayments {
		payment, err := (*response.FundingPayments)[i].normalize()
		if err != nil {
			return nil, err
		}
		payments = append(payments, payment)
	}
	return payments, nil
}

// wireFundingPayment is one row of GET /v4/fundingPayments.
type wireFundingPayment struct {
	CreatedAt *string `json:"createdAt"`
	Ticker    *string `json:"ticker"`
	Side      *string `json:"side"`
	Size      *string `json:"size"`
	Rate      *string `json:"rate"`
	Payment   *string `json:"payment"`
}

func (p *wireFundingPayment) validate() error {
	const object = "fundingPayments entry"
	if err := checkRequired(object,
		fieldCheck{"createdAt", p.CreatedAt != nil},
		fieldCheck{"ticker", p.Ticker != nil},
		fieldCheck{"side", p.Side != nil},
		fieldCheck{"size", p.Size != nil},
		fieldCheck{"rate", p.Rate != nil},
		fieldCheck{"payment", p.Payment != nil},
	); err != nil {
		return err
	}
	if *p.Side != positionSideLong && *p.Side != positionSideShort {
		return fmt.Errorf("dydx: %s has unknown side %q", object, *p.Side)
	}
	return nil
}

func (p *wireFundingPayment) normalize() (FundingPayment, error) {
	const object = "fundingPayments entry"
	createdAt, err := time.Parse(time.RFC3339, *p.CreatedAt)
	if err != nil {
		return FundingPayment{}, fmt.Errorf("dydx: %s has malformed createdAt %q: %w", object, *p.CreatedAt, err)
	}
	size, err := decimal.FromDecimalString(*p.Size)
	if err != nil {
		return FundingPayment{}, fmt.Errorf("dydx: %s size: %w", object, err)
	}
	rate, err := decimal.FromDecimalString(*p.Rate)
	if err != nil {
		return FundingPayment{}, fmt.Errorf("dydx: %s rate: %w", object, err)
	}
	payment, err := decimal.FromDecimalString(*p.Payment)
	if err != nil {
		return FundingPayment{}, fmt.Errorf("dydx: %s payment: %w", object, err)
	}
	return FundingPayment{
		CreatedAt: createdAt,
		Ticker:    *p.Ticker,
		Side:      *p.Side,
		Size:      size,
		Rate:      rate,
		Payment:   payment,
	}, nil
}

// fundingPaymentsResponse is GET /v4/fundingPayments.
type fundingPaymentsResponse struct {
	FundingPayments *[]wireFundingPayment `json:"fundingPayments"`
}

func (r *fundingPaymentsResponse) validate() error {
	if r.FundingPayments == nil {
		return missingField("fundingPayments response", "fundingPayments")
	}
	for i := range *r.FundingPayments {
		if err := (*r.FundingPayments)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}
