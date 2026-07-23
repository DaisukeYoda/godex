// godex-smoke runs the adoption-gate smoke test against a live venue
// (normally testnet) with real credentials taken from the environment.
//
// Usage:
//
//	LIGHTER_ACCOUNT_INDEX=... LIGHTER_API_KEY_INDEX=... LIGHTER_API_PRIVATE_KEY=... \
//	  go run ./cmd/godex-smoke -venue lighter -network testnet \
//	  -market-id 2 -symbol SOL-PERP -size 0.200 [-wait-fill] [-reconnect-check] [-record path.jsonl]
//
// Credentials must be venue-scoped trading API keys — never L1 master keys —
// and should be testnet-only. -record streams the raw account WS frames to a
// JSONL file (fixture refresh, auth-expiry observation).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/lighter"
	"github.com/DaisukeYoda/godex/smoketest"
	lighterclient "github.com/elliottech/lighter-go/client"
	lighterhttp "github.com/elliottech/lighter-go/client/http"
	"github.com/gorilla/websocket"
)

const (
	venueLighter = "lighter"

	// Body-level success code of the Lighter REST API.
	lighterRESTSuccessCode = 200
	// The server drops connections idle for 2 minutes; the recorder pings
	// every 30s like the executor does.
	recorderPingInterval = 30 * time.Second
	// Auth token TTL for the recorder's own subscription.
	recorderAuthTTL   = 10 * time.Minute
	tobRequestTimeout = 10 * time.Second
)

type options struct {
	venue          string
	network        string
	marketID       int64
	symbol         string
	size           decimal.Decimal
	waitFill       bool
	reconnectCheck bool
	recordPath     string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "godex-smoke: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch opts.venue {
	case venueLighter:
		return runLighter(ctx, opts)
	default:
		return fmt.Errorf("unknown venue %q (supported: %s)", opts.venue, venueLighter)
	}
}

func parseFlags(args []string) (options, error) {
	flags := flag.NewFlagSet("godex-smoke", flag.ContinueOnError)
	venue := flags.String("venue", "", "venue to test (required; supported: lighter)")
	network := flags.String("network", "", "testnet or mainnet (required; use testnet)")
	marketID := flags.Int64("market-id", -1, "venue market index (required; e.g. Lighter SOL testnet = 2)")
	symbol := flags.String("symbol", "", "normalized symbol label (required; e.g. SOL-PERP)")
	size := flags.String("size", "", "order size as a decimal string (required; e.g. 0.200)")
	waitFill := flags.Bool("wait-fill", false, "also wait for a natural near-touch maker fill")
	reconnectCheck := flags.Bool("reconnect-check", false, "force a reconnect mid-scenario and verify convergence")
	record := flags.String("record", "", "record raw account WS frames to this JSONL path")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if *venue == "" || *network == "" || *marketID < 0 || *symbol == "" || *size == "" {
		return options{}, fmt.Errorf("missing required flags: -venue, -network, -market-id, -symbol, -size are all required")
	}
	sizeDecimal, err := decimal.FromDecimalString(*size)
	if err != nil {
		return options{}, fmt.Errorf("invalid -size: %w", err)
	}
	return options{
		venue:          *venue,
		network:        *network,
		marketID:       *marketID,
		symbol:         *symbol,
		size:           sizeDecimal,
		waitFill:       *waitFill,
		reconnectCheck: *reconnectCheck,
		recordPath:     *record,
	}, nil
}

// requireEnv reads a required environment variable, failing fast when unset.
func requireEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("environment variable %s is required", name)
	}
	return value, nil
}

func logf(format string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, args...))
}

func loadLighterCredentials() (lighter.Credentials, error) {
	accountIndexRaw, err := requireEnv("LIGHTER_ACCOUNT_INDEX")
	if err != nil {
		return lighter.Credentials{}, err
	}
	accountIndex, err := strconv.ParseInt(accountIndexRaw, 10, 64)
	if err != nil {
		return lighter.Credentials{}, fmt.Errorf("invalid LIGHTER_ACCOUNT_INDEX: %w", err)
	}
	apiKeyIndexRaw, err := requireEnv("LIGHTER_API_KEY_INDEX")
	if err != nil {
		return lighter.Credentials{}, err
	}
	apiKeyIndex, err := strconv.ParseUint(apiKeyIndexRaw, 10, 8)
	if err != nil {
		return lighter.Credentials{}, fmt.Errorf("invalid LIGHTER_API_KEY_INDEX: %w", err)
	}
	apiPrivateKey, err := requireEnv("LIGHTER_API_PRIVATE_KEY")
	if err != nil {
		return lighter.Credentials{}, err
	}
	return lighter.Credentials{
		AccountIndex:  accountIndex,
		APIKeyIndex:   uint8(apiKeyIndex),
		APIPrivateKey: apiPrivateKey,
	}, nil
}

func runLighter(ctx context.Context, opts options) error {
	credentials, err := loadLighterCredentials()
	if err != nil {
		return err
	}
	network := lighter.Network(opts.network)
	restBaseURL, err := network.RESTBaseURL()
	if err != nil {
		return err
	}

	executor, err := lighter.New(lighter.Config{
		Credentials: credentials,
		Symbol:      godex.Symbol(opts.symbol),
		MarketID:    opts.marketID,
		Network:     network,
	})
	if err != nil {
		return err
	}

	stopRecorder := func() {}
	if opts.recordPath != "" {
		stopRecorder, err = startRecorder(ctx, opts, network, credentials)
		if err != nil {
			return fmt.Errorf("recorder: %w", err)
		}
	}
	defer stopRecorder()

	cfg := smoketest.Config{
		Symbol: godex.Symbol(opts.symbol),
		Size:   opts.size,
		FetchTOB: func(ctx context.Context) (smoketest.TOB, error) {
			return fetchLighterTOB(ctx, restBaseURL, opts.marketID)
		},
		Logf:     logf,
		WaitFill: opts.waitFill,
	}
	if opts.reconnectCheck {
		cfg.ForceReconnect = executor.ForceReconnect
	}
	if err := smoketest.Run(ctx, executor, cfg); err != nil {
		return err
	}
	logf("all adoption gates passed")
	return nil
}

// fetchLighterTOB reads the venue's top of book from the public order book
// endpoint (outside the executor contract, so the harness fetches directly).
func fetchLighterTOB(ctx context.Context, restBaseURL string, marketID int64) (smoketest.TOB, error) {
	requestCtx, cancel := context.WithTimeout(ctx, tobRequestTimeout)
	defer cancel()
	url := fmt.Sprintf("%s/api/v1/orderBookOrders?market_id=%d&limit=1", restBaseURL, marketID)
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
	if err != nil {
		return smoketest.TOB{}, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return smoketest.TOB{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return smoketest.TOB{}, fmt.Errorf("orderBookOrders failed: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return smoketest.TOB{}, err
	}
	var book struct {
		Code *int `json:"code"`
		Bids []struct {
			Price *string `json:"price"`
		} `json:"bids"`
		Asks []struct {
			Price *string `json:"price"`
		} `json:"asks"`
	}
	if err := json.Unmarshal(body, &book); err != nil {
		return smoketest.TOB{}, fmt.Errorf("orderBookOrders returned malformed JSON: %w", err)
	}
	if book.Code == nil || *book.Code != lighterRESTSuccessCode {
		return smoketest.TOB{}, fmt.Errorf("orderBookOrders returned unexpected code")
	}
	if len(book.Bids) == 0 || len(book.Asks) == 0 || book.Bids[0].Price == nil || book.Asks[0].Price == nil {
		return smoketest.TOB{}, fmt.Errorf("empty order book")
	}
	bestBid, err := decimal.FromDecimalString(*book.Bids[0].Price)
	if err != nil {
		return smoketest.TOB{}, err
	}
	bestAsk, err := decimal.FromDecimalString(*book.Asks[0].Price)
	if err != nil {
		return smoketest.TOB{}, err
	}
	return smoketest.TOB{BestBid: bestBid, BestAsk: bestAsk}, nil
}

// startRecorder subscribes to the account stream with its own auth token
// (independent of the executor's connection) and appends raw frames as JSONL.
func startRecorder(ctx context.Context, opts options, network lighter.Network, credentials lighter.Credentials) (func(), error) {
	wsURL, err := network.WSURL()
	if err != nil {
		return nil, err
	}
	restBaseURL, err := network.RESTBaseURL()
	if err != nil {
		return nil, err
	}
	chainID, err := network.ChainID()
	if err != nil {
		return nil, err
	}
	txClient, err := lighterclient.NewTxClient(lighterhttp.NewClient(restBaseURL),
		credentials.APIPrivateKey, credentials.AccountIndex, credentials.APIKeyIndex, chainID)
	if err != nil {
		return nil, err
	}
	auth, err := txClient.GetAuthToken(time.Now().Add(recorderAuthTTL))
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(opts.recordPath), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(opts.recordPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	subscribe, err := json.Marshal(map[string]string{
		"type":    "subscribe",
		"channel": fmt.Sprintf("account_all/%d", credentials.AccountIndex),
		"auth":    auth,
	})
	if err != nil {
		_ = conn.Close()
		_ = file.Close()
		return nil, err
	}
	if err := conn.WriteMessage(websocket.TextMessage, subscribe); err != nil {
		_ = conn.Close()
		_ = file.Close()
		return nil, err
	}
	logf("raw recorder: %s", opts.recordPath)

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			line, err := json.Marshal(map[string]any{
				"ts":  time.Now().UnixMilli(),
				"raw": string(data),
			})
			if err != nil {
				continue
			}
			if _, err := file.Write(append(line, '\n')); err != nil {
				logf("recorder write failed: %v", err)
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(recorderPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-readerDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"ping"}`))
			}
		}
	}()
	return func() {
		_ = conn.Close()
		<-readerDone
		_ = file.Close()
	}, nil
}
