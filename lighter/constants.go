package lighter

import (
	"regexp"
	"time"
)

// Venue endpoints and chain ids (signer_client.py: chain_id = 304 mainnet /
// 300 otherwise).
const (
	testnetRESTBaseURL = "https://testnet.zklighter.elliot.ai"
	testnetWSURL       = "wss://testnet.zklighter.elliot.ai/stream"
	mainnetRESTBaseURL = "https://mainnet.zklighter.elliot.ai"
	mainnetWSURL       = "wss://mainnet.zklighter.elliot.ai/stream"

	testnetChainID uint32 = 300
	mainnetChainID uint32 = 304
)

const (
	// REST bodies carry their own result code; errors arrive as HTTP 400
	// with a JSON body, so the body code — not the HTTP status — decides
	// sendTx success.
	restSuccessCode = 200

	// position.margin_mode value for cross margin. Liquidation-headroom math
	// uses whole-account equity, so only cross margin is supported.
	marginModeCross = 0

	// Only active markets accept orders.
	marketStatusActive = "active"
)

const (
	// The server drops connections after 2 minutes of silence; ping every 30s.
	pingInterval = 30 * time.Second
	// Account-WS subscription auth token TTL (signer default: 10 minutes).
	authTokenTTL = 600 * time.Second
	// Force-reconnect this margin before the auth token expires. Whether a
	// subscription survives token expiry is unverified upstream, so stay on
	// the conservative side and resubscribe with a fresh token.
	authRefreshMargin = 60 * time.Second

	// Margin/equity are not published on the account WS; poll REST.
	marginPollInterval = 5 * time.Second

	// Timeout for plain REST GETs (metadata, account, nonce), applied via
	// the default HTTP client.
	restRequestTimeout = 30 * time.Second

	// Max wait for sendTx and nonce resyncs. An outcome unknown after this
	// halts subsequent transactions (fault latch).
	defaultTxRequestTimeout = 10 * time.Second
	// Backoff before automatic fault recovery (nonce resync). While the
	// endpoint is unreachable the resync itself times out, so the effective
	// retry interval is this delay plus the request timeout.
	defaultTxFaultRecoveryDelay = 2 * time.Second

	// Retry the same tx at most once after an invalid-nonce resync.
	// Transactions are strictly serialized, so drift is rare (e.g. delayed
	// /nextNonce propagation); an invalid-nonce rejection is unprocessed, so
	// a single resend cannot double-submit. Fixed at 1 to rule out runaway
	// resends.
	nonceRetryLimit = 1

	// Max refetches when the initial REST snapshot loses the race against
	// newer WS state.
	initialSnapshotAttempts = 3
)

const (
	// Post-only (GTT) order lifetime. Local resolution of the shared-lib
	// "-1 = 28 days" sentinel; the venue caps expiries at 30 days.
	postOnlyOrderExpiry = 28 * 24 * time.Hour

	// USDC-notional comparison scale for the min_quote_amount check.
	quoteNotionalScale = 6
	// Lighter's maintenance margin fraction is an integer in 1/10000ths
	// (240 = 2.40%); decimal scale 4 maps it exactly onto a plain ratio.
	marginFractionScale = 4

	// Order status delivered on account_all_orders when a crossing
	// post-only is rejected (testnet observation 2026-07-05: sendTx accepts
	// the tx with code 200; the rejection arrives asynchronously with this
	// status).
	orderStatusPostOnlyCanceled = "canceled-post-only"

	// Wire timestamps at or above this value are epoch milliseconds; below,
	// epoch seconds.
	epochMsThreshold = 10_000_000_000
)

var (
	// Post-only rejection detection for synchronous sendTx error messages.
	postOnlyRejectPattern = regexp.MustCompile(`(?i)post.?only|maker.?only`)
	// lighter-python resyncs the nonce on "invalid nonce" errors.
	nonceErrorPattern = regexp.MustCompile(`(?i)invalid nonce`)
)

// Account WS control messages.
const (
	pingMessage = `{"type":"ping"}`
	pongMessage = `{"type":"pong"}`
)

// Market data.
const (
	// Funding applies hourly at the top of the hour
	// (docs.lighter.xyz/trading/funding).
	fundingIntervalHours = 1
	fundingResolution    = "1h"
	// The /fundings endpoint requires both start and end; look back two hours
	// so the window always contains at least one settled hourly funding.
	fundingLookback  = 2 * time.Hour
	fundingCountBack = 1
	// Funding direction values: the side that pays. "long" folds into a
	// positive rate (longs pay shorts).
	fundingDirectionLong  = "long"
	fundingDirectionShort = "short"

	// defaultCrossedBookGrace is how long a crossed book suppresses emits
	// before the connection is rebuilt. Nonce continuity should make crossing
	// impossible; this is defense in depth, and there is no official
	// per-market resubscribe to reach for instead. Kept short so consumers'
	// staleness thresholds see the outage quickly.
	defaultCrossedBookGrace = 1 * time.Second
)
