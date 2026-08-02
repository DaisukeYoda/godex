package dydx

import "time"

// Venue endpoints and chain ids. The Indexer serves market metadata and the
// account stream; the CometBFT RPC node serves block height, account lookups,
// and transaction broadcast. Those two hosts are the adapter's entire network
// surface — there is deliberately no Cosmos LCD dependency.
const (
	testnetIndexerRESTBaseURL = "https://indexer.v4testnet.dydx.exchange/v4"
	testnetIndexerWSURL       = "wss://indexer.v4testnet.dydx.exchange/v4/ws"
	testnetRPCBaseURL         = "https://test-dydx-rpc.kingnodes.com"
	mainnetIndexerRESTBaseURL = "https://indexer.dydx.trade/v4"
	mainnetIndexerWSURL       = "wss://indexer.dydx.trade/v4/ws"
	mainnetRPCBaseURL         = "https://dydx-ops-rpc.kingnodes.com:443"

	testnetChainID = "dydx-testnet-4"
	mainnetChainID = "dydx-mainnet-1"
)

// Address derivation.
const (
	// addressPrefix is the chain's bech32 human-readable part.
	addressPrefix = "dydx"
	// compressedPubKeyLen is the length of a compressed secp256k1 public key.
	compressedPubKeyLen = 33
	// signatureLen is the raw secp256k1 signature length Cosmos expects:
	// 32-byte R followed by 32-byte S, low-S normalized, never DER.
	signatureLen = 64
)

// Protobuf type URLs. These name the messages inside the transaction's Any
// wrappers and must match the chain's registry exactly.
const (
	msgPlaceOrderTypeURL  = "/dydxprotocol.clob.MsgPlaceOrder"
	msgCancelOrderTypeURL = "/dydxprotocol.clob.MsgCancelOrder"
	txExtensionTypeURL    = "/dydxprotocol.accountplus.TxExtension"
	pubKeyTypeURL         = "/cosmos.crypto.secp256k1.PubKey"
	baseAccountTypeURL    = "/cosmos.auth.v1beta1.BaseAccount"

	// accountQueryPath is the abci_query path for an account lookup.
	accountQueryPath = "/cosmos.auth.v1beta1.Query/Account"
)

// Short-term order parameters. godex places only short-term orders: they are
// gas-free, they are matched synchronously in CheckTx (so a crossing post-only
// is rejected in the broadcast response rather than asynchronously), and they
// never persist on chain.
const (
	// orderFlagsShortTerm marks an OrderId as short-term (32 = conditional,
	// 64 = long-term, neither of which godex places).
	orderFlagsShortTerm = 0
	// shortBlockForward is how far ahead of the current height good_til_block
	// is set, matching the official client's default.
	shortBlockForward = 15
	// shortBlockWindow is the chain's maximum order lifetime in blocks: a
	// good_til_block outside height+1 .. height+shortBlockWindow+1 is rejected.
	shortBlockWindow = 20
	// gasLimit is what the official client sets for short-term orders. They
	// are exempt from fees, so the fee coin list stays empty.
	gasLimit = 1_000_000
)

// Order enum values (dydxprotocol.clob).
const (
	sideBuy  = 1
	sideSell = 2

	timeInForceIOC      = 1
	timeInForcePostOnly = 2
)

// Venue result classification.
const (
	// txCodeOK is the CometBFT code for a transaction accepted into the
	// mempool. It means accepted, not filled.
	txCodeOK = 0
	// clobCodespace and clobErrPostOnlyWouldCross identify the synchronous
	// rejection of a post-only order that would have crossed the book — a
	// normal-path outcome, not an error.
	clobCodespace             = "clob"
	clobErrPostOnlyWouldCross = 2003

	// marketStatusActive is the only market status that accepts orders.
	marketStatusActive = "ACTIVE"

	// quoteAtomicResolution is the fixed atomic resolution of USDC quote
	// quantums, used in the subticks conversion.
	quoteAtomicResolution = -6
)

// Indexer enum strings.
const (
	positionSideLong  = "LONG"
	positionSideShort = "SHORT"

	fillSideBuy  = "BUY"
	fillSideSell = "SELL"
)

// Indexer order status and removal reason values used to normalize rejections.
const (
	orderStatusOpen               = "OPEN"
	orderStatusFilled             = "FILLED"
	orderStatusCanceled           = "CANCELED"
	orderStatusBestEffortCanceled = "BEST_EFFORT_CANCELED"

	removalReasonExpired        = "ORDER_REMOVAL_REASON_EXPIRED"
	removalReasonIndexerExpired = "ORDER_REMOVAL_REASON_INDEXER_EXPIRED"
)

// Market data.
const (
	// orderbookChannel is the Indexer's public order-book channel.
	orderbookChannel = "v4_orderbook"
	// fundingIntervalHours: dYdX v4 funding is an hourly rate
	// (perpetualMarkets.nextFundingRate).
	fundingIntervalHours = 1
	// firstMessageID: message_id starts at 0 (the connected message) and is
	// numbered per connection.
	firstMessageID = 0
	// messageReorderTolerance is the out-of-order buffer for message_id.
	// The Indexer numbers without gaps or duplicates, but arrival order can
	// swap across channels (observed: 4 swaps of adjacent ids in 7727
	// messages). Only an id that stays unfilled beyond this many early
	// arrivals is a true gap.
	messageReorderTolerance = 100
	// maxConsecutiveResyncs bounds per-market resubscribes
	// (unsubscribe→subscribe). Immediate resubscribing in a loop trips the
	// Indexer's subscribe rate limit ("Too many subscribe attempts ...
	// Please reconnect and try again"); past the bound the whole connection
	// is rebuilt, and reconnect backoff paces the retries naturally.
	maxConsecutiveResyncs = 3
)

// Indexer WebSocket protocol.
const (
	subaccountsChannel = "v4_subaccounts"

	wsTypeConnected    = "connected"
	wsTypeSubscribed   = "subscribed"
	wsTypeChannelData  = "channel_data"
	wsTypeUnsubscribed = "unsubscribed"
	wsTypeError        = "error"
	wsTypePong         = "pong"

	pingMessage = `{"type":"ping"}`
)

// Timings.
const (
	// The Indexer drops idle connections; ping well inside that window.
	pingInterval = 30 * time.Second

	// Block time is roughly one second. Poll often enough that the cached
	// height stays within a couple of blocks of the tip.
	defaultHeightPollInterval = 1 * time.Second
	// A height older than this is refused rather than used: good_til_block
	// derived from a stale height risks an already-expired order that looks
	// accepted locally. Kept far inside shortBlockWindow.
	defaultHeightStaleAfter = 5 * time.Second

	// Equity is only published in the stream's subscription snapshot, so
	// margin is refreshed over REST on this interval.
	snapshotPollInterval = 5 * time.Second

	// A position with size at a zero entry price is the transient the account
	// stream emits in the moment after a fill. REST is read from the same
	// Indexer state and can report it too, so a snapshot showing it is read up
	// to this many times, this far apart, before being taken at its word. The
	// observed correction arrived in the next update, within the same second.
	positionPriceReads       = 3
	positionPriceRereadDelay = 150 * time.Millisecond

	// Timeout for plain REST GETs (market metadata, fills, orders).
	restRequestTimeout = 30 * time.Second

	// Max wait for a broadcast or account query. An outcome unknown after this
	// halts subsequent transactions (fault latch).
	defaultTxRequestTimeout = 10 * time.Second
	// Backoff before automatic fault recovery (state reconciliation).
	defaultTxFaultRecoveryDelay = 2 * time.Second

	// How long a fill id stays in the duplicate-suppression cache. Comfortably
	// longer than any reconnect plus backfill window.
	fillDedupTTL = 1 * time.Hour
	// How long a venue order id stays resolvable after its order is gone, so a
	// fill the Indexer reports late is still attributable to the caller's order.
	venueOrderMappingTTL = 1 * time.Hour
	// Page size for the reconnect fill backfill.
	fillBackfillLimit = 100
	// Pages the backfill will walk back before giving up. Reaching this bound
	// means the outage produced more fills than the adapter can reconcile, which
	// is reported rather than silently truncated.
	maxFillBackfillPages = 20
)
