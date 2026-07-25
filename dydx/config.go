package dydx

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

// IndexerRESTBaseURL returns the Indexer REST endpoint for the network.
func (n Network) IndexerRESTBaseURL() (string, error) {
	switch n {
	case Testnet:
		return testnetIndexerRESTBaseURL, nil
	case Mainnet:
		return mainnetIndexerRESTBaseURL, nil
	default:
		return "", fmt.Errorf("dydx: unknown network %q", n)
	}
}

// IndexerWSURL returns the Indexer account/market WebSocket endpoint.
func (n Network) IndexerWSURL() (string, error) {
	switch n {
	case Testnet:
		return testnetIndexerWSURL, nil
	case Mainnet:
		return mainnetIndexerWSURL, nil
	default:
		return "", fmt.Errorf("dydx: unknown network %q", n)
	}
}

// RPCBaseURL returns the validator's CometBFT RPC endpoint, which serves block
// height, account lookups, and transaction broadcast.
func (n Network) RPCBaseURL() (string, error) {
	switch n {
	case Testnet:
		return testnetRPCBaseURL, nil
	case Mainnet:
		return mainnetRPCBaseURL, nil
	default:
		return "", fmt.Errorf("dydx: unknown network %q", n)
	}
}

// ChainID returns the signing chain id for the network.
func (n Network) ChainID() (string, error) {
	switch n {
	case Testnet:
		return testnetChainID, nil
	case Mainnet:
		return mainnetChainID, nil
	default:
		return "", fmt.Errorf("dydx: unknown network %q", n)
	}
}

// Credentials is the venue-scoped trading key material. The library never reads
// environment variables or files — pass values in from your own secret storage.
//
// dYdX has no separate API key: transactions are signed with an account key. Use
// a dedicated key registered on chain as a scoped authenticator (accountplus)
// and name it in AuthenticatorID, so the in-process key cannot withdraw or
// transfer. A key that controls the account outright must never reach a trading
// process.
type Credentials struct {
	// PrivateKeyHex is the hex-encoded secp256k1 signing key ("0x" prefix
	// optional).
	PrivateKeyHex string
	// Address is the bech32 account the orders belong to ("dydx1..."). Orders
	// are always attributed to this account; it is the message signer whether
	// or not a scoped key signs the transaction.
	//
	// Without an authenticator the signing key must control this address, and
	// Connect verifies that. With an authenticator the signing key is expected
	// to be a different, scoped key, so no such check applies.
	Address string
	// SubaccountNumber selects the subaccount to trade (0 is the default one).
	SubaccountNumber uint32
	// AuthenticatorID names the on-chain authenticator that authorizes this key
	// to act for Address. Nil means the key signs as the account owner itself.
	//
	// The chain expects exactly one authenticator id per message in a
	// transaction, and every transaction godex sends carries one message —
	// hence a single id rather than a list. Compose restrictions (message type,
	// market, subaccount) into one AllOf authenticator on chain and name that.
	AuthenticatorID *uint64
}

// Config parameterizes a dydx Executor.
type Config struct {
	Credentials Credentials
	// Symbol is the normalized label stamped on events (e.g. "ETH-PERP").
	Symbol godex.Symbol
	// Ticker is the venue market ticker (e.g. "ETH-USD"). dYdX identifies
	// markets by ticker; the numeric clob pair id is resolved at Connect.
	Ticker  string
	Network Network
	// Reconnect tunes the account WS; the zero value means
	// godex.DefaultReconnectConfig().
	Reconnect godex.ReconnectConfig
	// Logger receives operational logs; nil means slog.Default().
	Logger *slog.Logger

	// Test/ops overrides. Zero values resolve from Network and the package
	// constants.
	IndexerRESTBaseURL   string
	IndexerWSURL         string
	RPCBaseURL           string
	ChainID              string
	HTTPClient           *http.Client
	Now                  func() time.Time
	TxRequestTimeout     time.Duration
	TxFaultRecoveryDelay time.Duration
	HeightPollInterval   time.Duration
	HeightStaleAfter     time.Duration

	// newSigner is a test seam; nil uses the secp256k1 signer.
	newSigner func(cfg *resolvedConfig) (signer, error)
}

// resolvedConfig is Config with every default applied.
type resolvedConfig struct {
	credentials          Credentials
	symbol               godex.Symbol
	ticker               string
	indexerRESTBaseURL   string
	indexerWSURL         string
	rpcBaseURL           string
	chainID              string
	reconnect            godex.ReconnectConfig
	logger               *slog.Logger
	httpClient           *http.Client
	now                  func() time.Time
	txRequestTimeout     time.Duration
	txFaultRecoveryDelay time.Duration
	heightPollInterval   time.Duration
	heightStaleAfter     time.Duration
	newSigner            func(cfg *resolvedConfig) (signer, error)
}

// subaccountID is the Indexer's identifier for the configured subaccount, used
// both as the WebSocket subscription id and in REST queries.
func (c *resolvedConfig) subaccountID() string {
	return fmt.Sprintf("%s/%d", c.credentials.Address, c.credentials.SubaccountNumber)
}

func (c Config) resolve() (*resolvedConfig, error) {
	if c.Credentials.PrivateKeyHex == "" {
		return nil, fmt.Errorf("dydx: Credentials.PrivateKeyHex is required")
	}
	if c.Credentials.Address == "" {
		return nil, fmt.Errorf("dydx: Credentials.Address is required")
	}
	if !strings.HasPrefix(c.Credentials.Address, addressPrefix+"1") {
		return nil, fmt.Errorf("dydx: Credentials.Address %q is not a %s bech32 address",
			c.Credentials.Address, addressPrefix)
	}
	if c.Symbol == "" {
		return nil, fmt.Errorf("dydx: Symbol is required")
	}
	if c.Ticker == "" {
		return nil, fmt.Errorf("dydx: Ticker is required (e.g. \"ETH-USD\")")
	}
	if c.Network != Testnet && c.Network != Mainnet {
		return nil, fmt.Errorf("dydx: Network must be %q or %q, got %q", Testnet, Mainnet, c.Network)
	}

	resolved := &resolvedConfig{
		credentials:          c.Credentials,
		symbol:               c.Symbol,
		ticker:               c.Ticker,
		indexerRESTBaseURL:   c.IndexerRESTBaseURL,
		indexerWSURL:         c.IndexerWSURL,
		rpcBaseURL:           c.RPCBaseURL,
		chainID:              c.ChainID,
		reconnect:            c.Reconnect,
		logger:               c.Logger,
		httpClient:           c.HTTPClient,
		now:                  c.Now,
		txRequestTimeout:     c.TxRequestTimeout,
		txFaultRecoveryDelay: c.TxFaultRecoveryDelay,
		heightPollInterval:   c.HeightPollInterval,
		heightStaleAfter:     c.HeightStaleAfter,
		newSigner:            c.newSigner,
	}
	if resolved.indexerRESTBaseURL == "" {
		resolved.indexerRESTBaseURL, _ = c.Network.IndexerRESTBaseURL()
	}
	if resolved.indexerWSURL == "" {
		resolved.indexerWSURL, _ = c.Network.IndexerWSURL()
	}
	if resolved.rpcBaseURL == "" {
		resolved.rpcBaseURL, _ = c.Network.RPCBaseURL()
	}
	if resolved.chainID == "" {
		resolved.chainID, _ = c.Network.ChainID()
	}
	resolved.indexerRESTBaseURL = strings.TrimSuffix(resolved.indexerRESTBaseURL, "/")
	resolved.rpcBaseURL = strings.TrimSuffix(resolved.rpcBaseURL, "/")

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
		return nil, fmt.Errorf("dydx: TxRequestTimeout must be positive")
	}
	if resolved.txFaultRecoveryDelay == 0 {
		resolved.txFaultRecoveryDelay = defaultTxFaultRecoveryDelay
	}
	if resolved.txFaultRecoveryDelay <= 0 {
		return nil, fmt.Errorf("dydx: TxFaultRecoveryDelay must be positive")
	}
	if resolved.heightPollInterval == 0 {
		resolved.heightPollInterval = defaultHeightPollInterval
	}
	if resolved.heightPollInterval <= 0 {
		return nil, fmt.Errorf("dydx: HeightPollInterval must be positive")
	}
	if resolved.heightStaleAfter == 0 {
		resolved.heightStaleAfter = defaultHeightStaleAfter
	}
	if resolved.heightStaleAfter <= resolved.heightPollInterval {
		return nil, fmt.Errorf(
			"dydx: HeightStaleAfter (%s) must exceed HeightPollInterval (%s), otherwise every poll cycle reports a stale height",
			resolved.heightStaleAfter, resolved.heightPollInterval)
	}
	if resolved.newSigner == nil {
		resolved.newSigner = func(cfg *resolvedConfig) (signer, error) {
			return newKeySigner(cfg.credentials.PrivateKeyHex)
		}
	}
	return resolved, nil
}
