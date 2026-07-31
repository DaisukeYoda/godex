package hyperliquid

import (
	"regexp"
	"time"
)

var (
	// A post-only order the venue would have matched immediately is refused
	// synchronously ("Post only order would have immediately matched, bbo
	// was …"). That is a normal-path outcome, not an error.
	postOnlyRejectPattern = regexp.MustCompile(`(?i)post.?only`)
	// A cancel for an order the venue no longer holds ("Order was never
	// placed, already canceled, or filled") means the cancel's goal already
	// holds, which makes a retried cancel idempotent.
	cancelAlreadyGonePattern = regexp.MustCompile(`(?i)never placed|already cancel`)
)

// Venue endpoints. Hyperliquid serves REST (/info, /exchange) and the
// WebSocket from the same host per network.
const (
	testnetRESTBaseURL = "https://api.hyperliquid-testnet.xyz"
	testnetWSURL       = "wss://api.hyperliquid-testnet.xyz/ws"
	mainnetRESTBaseURL = "https://api.hyperliquid.xyz"
	mainnetWSURL       = "wss://api.hyperliquid.xyz/ws"

	// The phantom-agent "source" field separates mainnet and testnet
	// signatures so one network's signed action cannot be replayed on the
	// other ("a" = mainnet, "b" = testnet).
	mainnetSigningSource = "a"
	testnetSigningSource = "b"
)

// L1 action signing constants. The EIP-712 domain is fixed by the venue and
// is not an EVM chain: chain id 1337 and the zero verifying contract are
// literal protocol values, identical on mainnet and testnet (the network is
// distinguished by the agent source instead).
const (
	signingChainID       = 1337
	signingDomainName    = "Exchange"
	signingDomainVersion = "1"
)

const (
	infoPath     = "/info"
	exchangePath = "/exchange"
)

// /info query types the adapter issues.
const (
	infoTypeMeta               = "meta"
	infoTypeClearinghouseState = "clearinghouseState"
	infoTypeOrderStatus        = "orderStatus"
	// activeAssetData reports the account's margin mode for a coin even when
	// it holds no position there, which clearinghouseState cannot.
	infoTypeActiveAssetData = "activeAssetData"
	// extraAgents lists the agent wallets the account has approved.
	infoTypeExtraAgents = "extraAgents"
)

// Order wire vocabulary.
const (
	// tifALO is "add liquidity only" — the venue's post-only.
	tifALO = "Alo"
	tifIOC = "Ioc"

	// groupingNA marks a plain order with no TP/SL siblings.
	groupingNA = "na"

	actionTypeOrder         = "order"
	actionTypeCancelByCloid = "cancelByCloid"
)

// Wire side values: bid (buy) and ask (sell).
const (
	sideBid = "B"
	sideAsk = "A"
)

// Exchange response discriminators.
const (
	statusOK             = "ok"
	responseTypeOrder    = "order"
	responseTypeCancel   = "cancel"
	responseTypeDefault  = "default"
	orderStatusResting   = "resting"
	orderStatusFilledKey = "filled"
	orderStatusErrorKey  = "error"
)

// Price and size quantization. Perp prices carry at most five significant
// figures and at most (maxPerpPriceDecimals - szDecimals) decimal places;
// integer prices are always accepted regardless of significant figures.
const (
	maxPriceSigFigs      = 5
	maxPerpPriceDecimals = 6
)

const (
	// The venue closes connections after 60s of inactivity; the reference
	// clients ping every 50s. Application pings are used rather than
	// protocol ping frames because the documented keepalive is the former.
	pingInterval = 30 * time.Second

	// The clearinghouse-state subscription is push-driven, but nothing in
	// the venue contract promises a push for every account change. A REST
	// poll backstops it so position and margin converge even if a push is
	// missed; stale responses are discarded by observation sequence.
	accountPollInterval = 5 * time.Second

	// Timeout for plain REST /info posts (metadata, account state), applied
	// via the default HTTP client.
	restRequestTimeout = 30 * time.Second

	// Max wait for an /exchange submission. An outcome unknown after this
	// halts subsequent submissions (fault latch).
	defaultTxRequestTimeout = 10 * time.Second
	// Backoff before automatic fault recovery (open-order reconciliation).
	defaultTxFaultRecoveryDelay = 2 * time.Second

	// Max refetches when the initial REST snapshot loses the race against
	// newer WS state.
	initialSnapshotAttempts = 3

	// The venue answers a userFills subscription with a snapshot of recent
	// fills before any live update, so Connect can wait for it. Waiting is
	// what keeps a fill that lands moments after Connect from being mistaken
	// for pre-executor history and suppressed.
	initialFillSnapshotTimeout = 15 * time.Second
)

// wsMethodPing is the documented application-level keepalive; the venue
// answers on the "pong" channel.
const wsMethodPing = `{"method":"ping"}`

// WebSocket channels the adapter consumes.
const (
	channelSubscriptionResponse = "subscriptionResponse"
	channelPong                 = "pong"
	channelError                = "error"
	channelUserFills            = "userFills"
	channelOrderUpdates         = "orderUpdates"
)

// leverageTypeCross is the only margin mode the adapter supports:
// liquidation-headroom math is computed against whole-account equity.
const leverageTypeCross = "cross"

// orderStatus query outcomes used to reconcile an ambiguous submission.
const (
	queryStatusUnknownOid = "unknownOid"
	queryStatusOrder      = "order"
)

// Order lifecycle statuses reported on the orderUpdates channel.
//
// The set is enumerated rather than pattern-matched: an unrecognized status
// aborts the connection instead of being guessed at. Guessing has no safe
// default here — treating a live order as closed makes a strategy requote
// over its own resting quote, and treating a closed order as live leaves it
// waiting for a fill that will never come.
const (
	orderStatusOpen      = "open"
	orderStatusFilled    = "filled"
	orderStatusTriggered = "triggered"
)

// orderStatusClosed lists every status that ends an order without filling it
// in full. Each maps onto godex.OrderRejectedEvent.
var orderStatusClosed = map[string]struct{}{
	"canceled":                                  {},
	"rejected":                                  {},
	"marginCanceled":                            {},
	"vaultWithdrawalCanceled":                   {},
	"openInterestCapCanceled":                   {},
	"selfTradeCanceled":                         {},
	"reduceOnlyCanceled":                        {},
	"siblingFilledCanceled":                     {},
	"delistedCanceled":                          {},
	"liquidatedCanceled":                        {},
	"scheduledCancel":                           {},
	"tickRejected":                              {},
	"minTradeNtlRejected":                       {},
	"perpMarginRejected":                        {},
	"reduceOnlyRejected":                        {},
	"badAloPxRejected":                          {},
	"iocCancelRejected":                         {},
	"badTriggerPxRejected":                      {},
	"marketOrderNoLiquidityRejected":            {},
	"positionIncreaseAtOpenInterestCapRejected": {},
	"positionFlipAtOpenInterestCapRejected":     {},
	"tooAggressiveAtOpenInterestCapRejected":    {},
	"openInterestIncreaseRejected":              {},
	"insufficientSpotBalanceRejected":           {},
	"oracleRejected":                            {},
	"perpMaxPositionRejected":                   {},
}
