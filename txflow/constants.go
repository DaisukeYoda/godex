package txflow

import (
	"regexp"
	"time"
)

// TxFlow's platform API is not yet documented publicly ("Coming Soon" as of
// 2026-08). Every value below was observed from the mainnet API and the
// venue's own web client. Values a live order has not yet exercised carry an
// UNVERIFIED note; they are collected here so a correction is a one-line
// change.

var (
	// A post-only order the venue would have matched immediately is refused
	// synchronously. UNVERIFIED: the exact wording; the pattern follows the
	// Hyperliquid lineage ("Post only order would have immediately matched").
	postOnlyRejectPattern = regexp.MustCompile(`(?i)post.?only`)
	// A cancel for an order the venue no longer holds means the cancel's goal
	// already holds. UNVERIFIED wording, same lineage.
	cancelAlreadyGonePattern = regexp.MustCompile(`(?i)never placed|already cancel`)
)

// Venue endpoints. Mainnet serves REST (/info, /exchange) and the WebSocket
// from one host. The testnet hosts are not known (the testnet client is
// geo-restricted); Network.Testnet fails to resolve until they are supplied
// through Config overrides.
const (
	mainnetRESTBaseURL = "https://api.txflow.com"
	mainnetWSURL       = "wss://api.txflow.com/ws"
)

// L1 action signing constants. The EIP-712 domain name doubles as the
// network discriminator ("TxFlow-Mainnet"), the version is the API version,
// and the chain id is the venue's own L1 id — a signed action from one
// network is not replayable on another because all three are in the domain
// and in the Agent message.
const (
	mainnetSigningNetwork    = "TxFlow-Mainnet"
	mainnetSigningChainID    = 869
	mainnetSigningAPIVersion = 1
)

const (
	infoPath     = "/info"
	exchangePath = "/exchange"
)

// /info query types the adapter issues. The venue allowlists query types:
// anything else answers HTTP 403 "This action is not allowed." — notably
// meta, orderStatus, userFillsByTime and extraAgents, which the Hyperliquid
// adapter relies on and this one therefore cannot.
const (
	infoTypePerpMeta           = "perpMeta"
	infoTypeClearinghouseState = "clearinghouseState"
	infoTypeActiveAssetData    = "activeAssetData"
	infoTypeOpenOrders         = "openOrders"
	infoTypeHistoricalOrders   = "historicalOrders"
	infoTypeUserFills          = "userFills"
)

// Order wire vocabulary.
const (
	// tifALO is "add liquidity only" — the venue's post-only.
	tifALO = "Alo"
	tifIOC = "Ioc"

	// groupingNA marks a plain order with no TP/SL siblings.
	groupingNA = "na"

	actionTypeOrder = "order"
	// actionTypeCancel cancels by venue oid. UNVERIFIED: the venue's web
	// client was not observed cancelling; the shape follows the Hyperliquid
	// lineage ({type:"cancel", cancels:[{a, o}]}).
	actionTypeCancel = "cancel"
)

// Wire side values: bid (buy) and ask (sell).
const (
	sideBid = "B"
	sideAsk = "A"
)

// Exchange response discriminators. UNVERIFIED: the /exchange response
// envelope is assumed to follow the Hyperliquid lineage
// ({status:"ok"|"err", response:{type, data:{statuses:[...]}} | message}).
// Only these two are interpreted; any other status is an unknown outcome.
const (
	statusOK  = "ok"
	statusErr = "err"
)

const (
	// The venue answers {"method":"ping"} with {"method":"PONG"}. UNVERIFIED:
	// the idle-disconnect window; 30s keeps well inside the Hyperliquid
	// lineage's 60s.
	pingInterval = 30 * time.Second

	// Position and margin are read by REST; a poll backstops the
	// fill-triggered refresh so they converge after changes this executor
	// did not cause.
	accountPollInterval = 5 * time.Second

	// The venue has no fills stream: executions are read from the userFills
	// query, so it is polled continuously. UNVERIFIED: the venue's rate
	// limits; 2s is a guess that keeps well under one request per second.
	fillPollInterval = 2 * time.Second

	// Timeout for plain REST /info posts, applied via the default HTTP client.
	restRequestTimeout = 30 * time.Second

	// Max wait for an /exchange submission. An outcome unknown after this
	// halts subsequent submissions (fault latch).
	defaultTxRequestTimeout = 10 * time.Second
	// Backoff before automatic fault recovery (order reconciliation).
	defaultTxFaultRecoveryDelay = 2 * time.Second

	// Max refetches when the initial REST snapshot reports a position in
	// flight (size at no price).
	initialSnapshotAttempts = 3
)

// wsMethodPing is the application-level keepalive.
const wsMethodPing = `{"method":"ping"}`

// WebSocket message discriminators the adapter consumes. Data frames carry a
// "channel"; the pong carries only "method".
const (
	wsMethodPong                = "PONG"
	channelSubscriptionResponse = "subscriptionResponse"
	channelError                = "error"
	channelOrderUpdates         = "orderUpdates"
)

// leverageTypeCross is the only margin mode the adapter supports:
// liquidation-headroom math is computed against whole-account equity. The
// venue spells it capitalized, in both clearinghouseState and activeAssetData.
const leverageTypeCross = "Cross"

// Order lifecycle statuses, as reported on the orderUpdates channel and in
// historicalOrders' order.status. (historicalOrders also carries a top-level
// status in lowerCamel; the adapter reads order.status only.)
//
// The set is enumerated rather than pattern-matched: an unrecognized status
// aborts the connection instead of being guessed at. Guessing has no safe
// default — treating a live order as closed makes a strategy requote over its
// own resting quote, and treating a closed order as live leaves it waiting
// for a fill that will never come.
const (
	// orderStatusOpen is UNVERIFIED: only PartialFilled was observed for a
	// resting order; a resting order with no fills presumably reads "Open".
	orderStatusOpen          = "Open"
	orderStatusPartialFilled = "PartialFilled"
	orderStatusFilled        = "Filled"
	orderStatusTriggered     = "Triggered"
)

// orderStatusClosed lists every status that ends an order without filling it
// in full. Each maps onto godex.OrderRejectedEvent.
var orderStatusClosed = map[string]struct{}{
	"Canceled":        {},
	"PartialCanceled": {},
	"Rejected":        {},
}
