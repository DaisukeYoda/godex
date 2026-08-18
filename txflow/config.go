package txflow

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/internal/evmsign"
)

// Network selects the venue deployment. There is no default: the caller must
// choose explicitly (fail fast).
type Network string

// Networks. Testnet is named so a caller can select it, but its endpoints and
// signing parameters are not known: resolving it fails unless every override
// (RESTBaseURL, WSURL, SigningNetwork, SigningChainID, SigningAPIVersion) is
// supplied.
const (
	Testnet Network = "testnet"
	Mainnet Network = "mainnet"
)

func (n Network) unknownError(what string) error {
	switch n {
	case Testnet:
		return fmt.Errorf("txflow: testnet %s is not known; supply it through Config overrides", what)
	default:
		return fmt.Errorf("txflow: unknown network %q", n)
	}
}

// RESTBaseURL returns the venue's REST host for the network.
func (n Network) RESTBaseURL() (string, error) {
	if n == Mainnet {
		return mainnetRESTBaseURL, nil
	}
	return "", n.unknownError("REST endpoint")
}

// WSURL returns the venue's WebSocket endpoint for the network.
func (n Network) WSURL() (string, error) {
	if n == Mainnet {
		return mainnetWSURL, nil
	}
	return "", n.unknownError("WebSocket endpoint")
}

// SigningParams returns the EIP-712 parameters that scope a signature to
// this network: the domain name (which is also the Agent's txflowNetwork),
// the chain id, and the API version (which is also the domain version).
func (n Network) SigningParams() (SigningParams, error) {
	if n == Mainnet {
		return SigningParams{
			Network:    mainnetSigningNetwork,
			ChainID:    mainnetSigningChainID,
			APIVersion: mainnetSigningAPIVersion,
		}, nil
	}
	return SigningParams{}, n.unknownError("signing parameters")
}

// SigningParams is the network-scoped half of the signing preimage.
type SigningParams struct {
	Network    string
	ChainID    uint32
	APIVersion uint32
}

func (p SigningParams) complete() bool {
	return p.Network != "" && p.ChainID != 0 && p.APIVersion != 0
}

// Credentials is the venue-scoped trading key material. The library never
// reads environment variables or files — pass values in from your own secret
// storage.
type Credentials struct {
	// AccountAddress is the 0x address whose positions and margin are
	// traded and observed. With a trading (agent) wallet this is the master
	// account, not the agent's own address.
	AccountAddress string
	// APIPrivateKey is the hex-encoded secp256k1 key that signs actions
	// ("0x" prefix optional). Use a TxFlow trading wallet approved for the
	// account: it can trade but cannot withdraw. A master key must never
	// reach this process.
	APIPrivateKey string
}

// Config parameterizes a txflow Executor.
type Config struct {
	Credentials Credentials
	// Symbol is the normalized label stamped on events (e.g. "BTC-PERP").
	Symbol godex.Symbol
	// Market is the venue-native perp name the symbol maps to, as listed by
	// perpMeta (e.g. "BTC-USDC").
	Market  string
	Network Network
	// MaintenanceMarginFraction is the maintenance margin rate reported in
	// godex.ExecutionMetadata. The venue documents tiered, per-market
	// maintenance rates but exposes them through no query this adapter has
	// found, so the caller supplies the strictest tier for the market. It is
	// required: the adapter will not invent one.
	MaintenanceMarginFraction decimal.Decimal
	// Reconnect tunes the account WS; the zero value means
	// godex.DefaultReconnectConfig().
	Reconnect godex.ReconnectConfig
	// Logger receives operational logs; nil means slog.Default().
	Logger *slog.Logger

	// Test/ops overrides. Zero values resolve from Network and the package
	// constants.
	RESTBaseURL          string
	WSURL                string
	Signing              SigningParams
	HTTPClient           *http.Client
	Now                  func() time.Time
	TxRequestTimeout     time.Duration
	TxFaultRecoveryDelay time.Duration
	AccountPollInterval  time.Duration
	FillPollInterval     time.Duration

	// newSigner is a test seam; nil uses the in-process key signer.
	newSigner func(cfg *resolvedConfig) (signer, error)
}

// resolvedConfig is Config with every default applied.
type resolvedConfig struct {
	credentials    Credentials
	symbol         godex.Symbol
	market         string
	accountAddress string
	marginFraction decimal.Decimal

	restBaseURL          string
	wsURL                string
	signing              SigningParams
	reconnect            godex.ReconnectConfig
	logger               *slog.Logger
	httpClient           *http.Client
	now                  func() time.Time
	txRequestTimeout     time.Duration
	txFaultRecoveryDelay time.Duration
	accountPollInterval  time.Duration
	fillPollInterval     time.Duration
	newSigner            func(cfg *resolvedConfig) (signer, error)
}

func (c Config) resolve() (*resolvedConfig, error) {
	if c.Credentials.APIPrivateKey == "" {
		return nil, fmt.Errorf("txflow: Credentials.APIPrivateKey is required")
	}
	if c.Credentials.AccountAddress == "" {
		return nil, fmt.Errorf("txflow: Credentials.AccountAddress is required")
	}
	accountAddress, err := evmsign.ParseAddress(c.Credentials.AccountAddress)
	if err != nil {
		return nil, fmt.Errorf("txflow: %w", err)
	}
	if c.Symbol == "" {
		return nil, fmt.Errorf("txflow: Symbol is required")
	}
	if c.Market == "" {
		return nil, fmt.Errorf("txflow: Market is required")
	}
	if c.Network != Testnet && c.Network != Mainnet {
		return nil, fmt.Errorf("txflow: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
	}
	if c.MaintenanceMarginFraction.Sign() <= 0 || c.MaintenanceMarginFraction.Cmp(decimal.New(1, 0)) >= 0 {
		return nil, fmt.Errorf("txflow: MaintenanceMarginFraction must be in (0, 1), got %s",
			c.MaintenanceMarginFraction)
	}

	resolved := &resolvedConfig{
		credentials:          c.Credentials,
		symbol:               c.Symbol,
		market:               c.Market,
		accountAddress:       evmsign.NormalizeAddress(accountAddress),
		marginFraction:       c.MaintenanceMarginFraction,
		restBaseURL:          c.RESTBaseURL,
		wsURL:                c.WSURL,
		signing:              c.Signing,
		reconnect:            c.Reconnect,
		logger:               c.Logger,
		httpClient:           c.HTTPClient,
		now:                  c.Now,
		txRequestTimeout:     c.TxRequestTimeout,
		txFaultRecoveryDelay: c.TxFaultRecoveryDelay,
		accountPollInterval:  c.AccountPollInterval,
		fillPollInterval:     c.FillPollInterval,
		newSigner:            c.newSigner,
	}
	if resolved.restBaseURL == "" {
		if resolved.restBaseURL, err = c.Network.RESTBaseURL(); err != nil {
			return nil, err
		}
	}
	if resolved.wsURL == "" {
		if resolved.wsURL, err = c.Network.WSURL(); err != nil {
			return nil, err
		}
	}
	if !resolved.signing.complete() {
		if resolved.signing != (SigningParams{}) {
			return nil, fmt.Errorf("txflow: Signing override must set Network, ChainID and APIVersion together")
		}
		if resolved.signing, err = c.Network.SigningParams(); err != nil {
			return nil, err
		}
	}
	if resolved.reconnect.IsZero() {
		resolved.reconnect = godex.DefaultReconnectConfig()
	}
	if err := resolved.reconnect.Validate(); err != nil {
		return nil, err
	}
	if resolved.logger == nil {
		resolved.logger = slog.Default()
	}
	if resolved.httpClient == nil {
		resolved.httpClient = &http.Client{Timeout: restRequestTimeout}
	}
	if resolved.now == nil {
		resolved.now = time.Now
	}
	for _, check := range []struct {
		name  string
		value *time.Duration
		def   time.Duration
	}{
		{"TxRequestTimeout", &resolved.txRequestTimeout, defaultTxRequestTimeout},
		{"TxFaultRecoveryDelay", &resolved.txFaultRecoveryDelay, defaultTxFaultRecoveryDelay},
		{"AccountPollInterval", &resolved.accountPollInterval, accountPollInterval},
		{"FillPollInterval", &resolved.fillPollInterval, fillPollInterval},
	} {
		if *check.value == 0 {
			*check.value = check.def
		}
		if *check.value <= 0 {
			return nil, fmt.Errorf("txflow: %s must be positive", check.name)
		}
	}
	if resolved.newSigner == nil {
		resolved.newSigner = func(cfg *resolvedConfig) (signer, error) {
			return newKeySigner(cfg.credentials.APIPrivateKey, cfg.signing)
		}
	}
	return resolved, nil
}
