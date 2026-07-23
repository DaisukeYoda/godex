package lighter

import (
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/DaisukeYoda/godex"
)

// Network selects the venue deployment. There is no default: the caller must
// choose explicitly (fail fast).
type Network string

// Networks.
const (
	Testnet Network = "testnet"
	Mainnet Network = "mainnet"
)

// RESTBaseURL returns the venue's public REST endpoint for the network.
func (n Network) RESTBaseURL() (string, error) {
	switch n {
	case Testnet:
		return testnetRESTBaseURL, nil
	case Mainnet:
		return mainnetRESTBaseURL, nil
	default:
		return "", fmt.Errorf("lighter: unknown network %q", n)
	}
}

// WSURL returns the venue's account/market WebSocket endpoint for the network.
func (n Network) WSURL() (string, error) {
	switch n {
	case Testnet:
		return testnetWSURL, nil
	case Mainnet:
		return mainnetWSURL, nil
	default:
		return "", fmt.Errorf("lighter: unknown network %q", n)
	}
}

// ChainID returns the signing chain id for the network.
func (n Network) ChainID() (uint32, error) {
	switch n {
	case Testnet:
		return testnetChainID, nil
	case Mainnet:
		return mainnetChainID, nil
	default:
		return 0, fmt.Errorf("lighter: unknown network %q", n)
	}
}

// Credentials is the venue-scoped trading key material. The library never
// reads environment variables or files — pass values in from your own secret
// storage. Use trading-only API keys; never put withdrawal-capable L1 keys
// anywhere near this process.
type Credentials struct {
	AccountIndex int64
	APIKeyIndex  uint8
	// APIPrivateKey is the hex-encoded, on-chain-registered trading API key
	// ("0x" prefix optional).
	APIPrivateKey string
}

// Config parameterizes a lighter Executor.
type Config struct {
	Credentials Credentials
	// Symbol is the normalized label stamped on events (e.g. "SOL-PERP").
	Symbol godex.Symbol
	// MarketID is the Lighter market index (e.g. SOL testnet = 2).
	MarketID int64
	Network  Network
	// Reconnect tunes the account WS; the zero value means
	// godex.DefaultReconnectConfig().
	Reconnect godex.ReconnectConfig
	// Logger receives operational logs; nil means slog.Default().
	Logger *slog.Logger

	// Test/ops overrides. Zero values resolve from Network and the package
	// constants.
	RESTBaseURL          string
	WSURL                string
	ChainID              uint32
	HTTPClient           *http.Client
	Now                  func() time.Time
	TxRequestTimeout     time.Duration
	TxFaultRecoveryDelay time.Duration

	// newSigner is a test seam; nil uses the lighter-go signer.
	newSigner func(cfg *resolvedConfig) (signer, error)
}

// resolvedConfig is Config with every default applied.
type resolvedConfig struct {
	credentials          Credentials
	symbol               godex.Symbol
	marketID             int64
	marketIndex          int16 // marketID as the wire order type
	restBaseURL          string
	wsURL                string
	chainID              uint32
	reconnect            godex.ReconnectConfig
	logger               *slog.Logger
	httpClient           *http.Client
	now                  func() time.Time
	txRequestTimeout     time.Duration
	txFaultRecoveryDelay time.Duration
	newSigner            func(cfg *resolvedConfig) (signer, error)
}

func (c Config) resolve() (*resolvedConfig, error) {
	if c.Credentials.APIPrivateKey == "" {
		return nil, fmt.Errorf("lighter: Credentials.APIPrivateKey is required")
	}
	if c.Credentials.AccountIndex < 0 {
		return nil, fmt.Errorf("lighter: Credentials.AccountIndex must be non-negative")
	}
	if c.Symbol == "" {
		return nil, fmt.Errorf("lighter: Symbol is required")
	}
	if c.MarketID < 0 || c.MarketID > math.MaxInt16 {
		return nil, fmt.Errorf("lighter: MarketID %d is outside the venue's int16 market index range", c.MarketID)
	}
	if c.Network != Testnet && c.Network != Mainnet {
		return nil, fmt.Errorf("lighter: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
	}

	resolved := &resolvedConfig{
		credentials:          c.Credentials,
		symbol:               c.Symbol,
		marketID:             c.MarketID,
		marketIndex:          int16(c.MarketID),
		restBaseURL:          c.RESTBaseURL,
		wsURL:                c.WSURL,
		chainID:              c.ChainID,
		reconnect:            c.Reconnect,
		logger:               c.Logger,
		httpClient:           c.HTTPClient,
		now:                  c.Now,
		txRequestTimeout:     c.TxRequestTimeout,
		txFaultRecoveryDelay: c.TxFaultRecoveryDelay,
		newSigner:            c.newSigner,
	}
	if resolved.restBaseURL == "" {
		resolved.restBaseURL, _ = c.Network.RESTBaseURL()
	}
	if resolved.wsURL == "" {
		resolved.wsURL, _ = c.Network.WSURL()
	}
	if resolved.chainID == 0 {
		resolved.chainID, _ = c.Network.ChainID()
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
	if resolved.txRequestTimeout == 0 {
		resolved.txRequestTimeout = defaultTxRequestTimeout
	}
	if resolved.txRequestTimeout <= 0 {
		return nil, fmt.Errorf("lighter: TxRequestTimeout must be positive")
	}
	if resolved.txFaultRecoveryDelay == 0 {
		resolved.txFaultRecoveryDelay = defaultTxFaultRecoveryDelay
	}
	if resolved.txFaultRecoveryDelay <= 0 {
		return nil, fmt.Errorf("lighter: TxFaultRecoveryDelay must be positive")
	}
	if resolved.newSigner == nil {
		resolved.newSigner = func(cfg *resolvedConfig) (signer, error) {
			return newTxSigner(cfg.restBaseURL, cfg.credentials.APIPrivateKey,
				cfg.credentials.AccountIndex, cfg.credentials.APIKeyIndex, cfg.chainID)
		}
	}
	return resolved, nil
}
