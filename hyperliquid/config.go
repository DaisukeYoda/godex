package hyperliquid

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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

// RESTBaseURL returns the venue's REST host for the network.
func (n Network) RESTBaseURL() (string, error) {
	switch n {
	case Testnet:
		return testnetRESTBaseURL, nil
	case Mainnet:
		return mainnetRESTBaseURL, nil
	default:
		return "", fmt.Errorf("hyperliquid: unknown network %q", n)
	}
}

// WSURL returns the venue's WebSocket endpoint for the network.
func (n Network) WSURL() (string, error) {
	switch n {
	case Testnet:
		return testnetWSURL, nil
	case Mainnet:
		return mainnetWSURL, nil
	default:
		return "", fmt.Errorf("hyperliquid: unknown network %q", n)
	}
}

// SigningSource returns the phantom-agent source that scopes a signature to
// this network.
func (n Network) SigningSource() (string, error) {
	switch n {
	case Testnet:
		return testnetSigningSource, nil
	case Mainnet:
		return mainnetSigningSource, nil
	default:
		return "", fmt.Errorf("hyperliquid: unknown network %q", n)
	}
}

// Credentials is the venue-scoped trading key material. The library never
// reads environment variables or files — pass values in from your own secret
// storage.
type Credentials struct {
	// AccountAddress is the 0x address whose positions and margin are
	// traded and observed. With an API (agent) wallet this is the master
	// account, not the agent's own address.
	AccountAddress string
	// APIPrivateKey is the hex-encoded secp256k1 key that signs actions
	// ("0x" prefix optional). Use a Hyperliquid API wallet: it can trade but
	// cannot withdraw or transfer. A master key must never reach this
	// process.
	APIPrivateKey string
	// VaultAddress optionally routes orders to a vault or subaccount. When
	// set, account state is observed for the vault rather than for
	// AccountAddress.
	VaultAddress string
}

// Config parameterizes a hyperliquid Executor.
type Config struct {
	Credentials Credentials
	// Symbol is the normalized label stamped on events (e.g. "ETH-PERP").
	Symbol godex.Symbol
	// Coin is the venue-native perp name the symbol maps to (e.g. "ETH").
	Coin    string
	Network Network
	// Reconnect tunes the account WS; the zero value means
	// godex.DefaultReconnectConfig().
	Reconnect godex.ReconnectConfig
	// Logger receives operational logs; nil means slog.Default().
	Logger *slog.Logger

	// Test/ops overrides. Zero values resolve from Network and the package
	// constants.
	RESTBaseURL          string
	WSURL                string
	SigningSource        string
	HTTPClient           *http.Client
	Now                  func() time.Time
	TxRequestTimeout     time.Duration
	TxFaultRecoveryDelay time.Duration
	AccountPollInterval  time.Duration
	FillSnapshotTimeout  time.Duration

	// newSigner is a test seam; nil uses the in-process key signer.
	newSigner func(cfg *resolvedConfig) (signer, error)
}

// resolvedConfig is Config with every default applied.
type resolvedConfig struct {
	credentials Credentials
	symbol      godex.Symbol
	coin        string
	// accountAddress is the trading account itself. Agent approvals live on
	// it even when orders are routed to a vault.
	accountAddress string
	// userAddress is the account whose state is observed: the vault when one
	// is configured, otherwise the trading account.
	userAddress  string
	vaultAddress []byte

	restBaseURL          string
	wsURL                string
	signingSource        string
	reconnect            godex.ReconnectConfig
	logger               *slog.Logger
	httpClient           *http.Client
	now                  func() time.Time
	txRequestTimeout     time.Duration
	txFaultRecoveryDelay time.Duration
	accountPollInterval  time.Duration
	fillSnapshotTimeout  time.Duration
	newSigner            func(cfg *resolvedConfig) (signer, error)
}

func (c Config) resolve() (*resolvedConfig, error) {
	if c.Credentials.APIPrivateKey == "" {
		return nil, fmt.Errorf("hyperliquid: Credentials.APIPrivateKey is required")
	}
	if c.Credentials.AccountAddress == "" {
		return nil, fmt.Errorf("hyperliquid: Credentials.AccountAddress is required")
	}
	accountAddress, err := parseAddress(c.Credentials.AccountAddress)
	if err != nil {
		return nil, err
	}
	if c.Symbol == "" {
		return nil, fmt.Errorf("hyperliquid: Symbol is required")
	}
	if c.Coin == "" {
		return nil, fmt.Errorf("hyperliquid: Coin is required")
	}
	if c.Network != Testnet && c.Network != Mainnet {
		return nil, fmt.Errorf("hyperliquid: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
	}

	resolved := &resolvedConfig{
		credentials:          c.Credentials,
		symbol:               c.Symbol,
		coin:                 c.Coin,
		accountAddress:       normalizeAddress(accountAddress),
		userAddress:          normalizeAddress(accountAddress),
		restBaseURL:          c.RESTBaseURL,
		wsURL:                c.WSURL,
		signingSource:        c.SigningSource,
		reconnect:            c.Reconnect,
		logger:               c.Logger,
		httpClient:           c.HTTPClient,
		now:                  c.Now,
		txRequestTimeout:     c.TxRequestTimeout,
		txFaultRecoveryDelay: c.TxFaultRecoveryDelay,
		accountPollInterval:  c.AccountPollInterval,
		fillSnapshotTimeout:  c.FillSnapshotTimeout,
		newSigner:            c.newSigner,
	}
	if c.Credentials.VaultAddress != "" {
		vault, err := parseAddress(c.Credentials.VaultAddress)
		if err != nil {
			return nil, err
		}
		resolved.vaultAddress = vault
		resolved.userAddress = normalizeAddress(vault)
	}
	if resolved.restBaseURL == "" {
		resolved.restBaseURL, _ = c.Network.RESTBaseURL()
	}
	if resolved.wsURL == "" {
		resolved.wsURL, _ = c.Network.WSURL()
	}
	if resolved.signingSource == "" {
		resolved.signingSource, _ = c.Network.SigningSource()
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
		return nil, fmt.Errorf("hyperliquid: TxRequestTimeout must be positive")
	}
	if resolved.txFaultRecoveryDelay == 0 {
		resolved.txFaultRecoveryDelay = defaultTxFaultRecoveryDelay
	}
	if resolved.txFaultRecoveryDelay <= 0 {
		return nil, fmt.Errorf("hyperliquid: TxFaultRecoveryDelay must be positive")
	}
	if resolved.accountPollInterval == 0 {
		resolved.accountPollInterval = accountPollInterval
	}
	if resolved.accountPollInterval <= 0 {
		return nil, fmt.Errorf("hyperliquid: AccountPollInterval must be positive")
	}
	if resolved.fillSnapshotTimeout == 0 {
		resolved.fillSnapshotTimeout = initialFillSnapshotTimeout
	}
	if resolved.fillSnapshotTimeout <= 0 {
		return nil, fmt.Errorf("hyperliquid: FillSnapshotTimeout must be positive")
	}
	if resolved.newSigner == nil {
		resolved.newSigner = func(cfg *resolvedConfig) (signer, error) {
			return newKeySigner(cfg.credentials.APIPrivateKey, cfg.signingSource, cfg.vaultAddress)
		}
	}
	return resolved, nil
}

// normalizeAddress renders address bytes in the lowercase 0x form the venue
// echoes back, so equality checks against streamed payloads are exact rather
// than case-sensitive comparisons of user input.
func normalizeAddress(address []byte) string {
	var builder strings.Builder
	builder.WriteString("0x")
	const hexDigits = "0123456789abcdef"
	for _, value := range address {
		builder.WriteByte(hexDigits[value>>4])
		builder.WriteByte(hexDigits[value&0x0f])
	}
	return builder.String()
}
