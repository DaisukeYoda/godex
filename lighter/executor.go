// Package lighter implements godex.VenueExecutor for Lighter (zkLighter).
// It signs LIMIT orders (post-only / IOC) with an on-chain-registered trading
// API key via the official lighter-go SDK and submits them through sendTx.
//
// Known gap: a cancel the caller asks for produces no OrderRejectedEvent for a
// resting order. sendTx accepting the transaction is receipt, not application
// — this venue accepts and then rejects asynchronously — so the adapter waits
// for the venue to say the order ended. The account stream reports only the
// post-only cancellation status, and there is no order-status endpoint to ask
// instead, so a plain cancellation is never observed. Such orders also stay
// tracked, since nothing retires them.
//
// Closing it needs the venue's order-status vocabulary, which is not
// documented here and must not be guessed at: mislabelling a status decides
// whether a fill is suppressed. `cmd/godex-smoke -record` captures the raw
// account frames a testnet session produces, which is how that vocabulary is
// meant to be established.
package lighter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/internal/dedupe"
	"github.com/DaisukeYoda/godex/internal/ws"
	"github.com/elliottech/lighter-go/types/txtypes"
)

const wsLabel = "lighter-account"

type accountInvalidObservation struct {
	err      error
	sequence int64
}

// Executor is the Lighter implementation of godex.VenueExecutor.
type Executor struct {
	cfg    *resolvedConfig
	logger *slog.Logger

	events          chan godex.AccountEvent
	rejections      *dedupe.Set[godex.OrderID]
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	// opMu guards closed/connected and the in-flight operation accounting
	// that lets Close wait for every potential event emitter before closing
	// the events channel.
	opMu      sync.Mutex
	opWG      sync.WaitGroup
	closed    bool
	connected bool

	// txMu serializes take-nonce → sign → sendTx → resync so nonce
	// allocation order equals submission order (the venue requires strictly
	// increasing nonces per api key). It also guards the fault latch.
	txMu        sync.Mutex
	nonce       *nonceManager
	txFault     error
	faultTimer  *time.Timer
	acceptingTx bool

	signer signer
	socket *ws.Socket
	meta   *orderBookDetail

	// stateMu guards account-observation bookkeeping, order tracking, and
	// event emission (holding it through emission preserves per-batch event
	// ordering).
	stateMu                sync.Mutex
	observationSeq         int64
	lastStateObservation   int64
	pendingSubscriptionSeq int64 // 0 = none
	hasPositionSnapshot    bool
	hasMarginSnapshot      bool
	accountInvalid         *accountInvalidObservation
	orders                 map[godex.OrderID]int64 // OrderID -> client order index
	// canceling holds orders whose cancel the venue accepted. Acceptance here
	// is receipt of the transaction, not the order having ended by it — this
	// venue rejects asynchronously — so the order stays tracked. Membership
	// makes a second cancel unaddressable and labels the terminal event when
	// the account stream reports one.
	canceling          map[godex.OrderID]struct{}
	clientOrderCounter int64
	authRefreshTimer   *time.Timer

	pollerWG sync.WaitGroup
}

var _ godex.VenueExecutor = (*Executor)(nil)

// New builds an Executor. It performs no I/O; Connect does.
func New(cfg Config) (*Executor, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	return &Executor{
		cfg:             resolved,
		logger:          resolved.logger,
		events:          make(chan godex.AccountEvent, godex.DefaultAccountEventBuffer),
		rejections:      dedupe.NewSet[godex.OrderID](dedupe.RejectionCapacity),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
		orders:          make(map[godex.OrderID]int64),
		canceling:       make(map[godex.OrderID]struct{}),
		// Seeded from wall-clock ms so client order indexes stay unique
		// across restarts without persistence.
		clientOrderCounter: resolved.now().UnixMilli(),
	}, nil
}

// VenueID implements godex.VenueExecutor.
func (e *Executor) VenueID() godex.VenueID {
	return godex.VenueLighter
}

// AccountEvents implements godex.VenueExecutor.
func (e *Executor) AccountEvents() <-chan godex.AccountEvent {
	return e.events
}

// Connect implements godex.VenueExecutor: it loads market metadata, validates
// the API key, initializes the nonce, starts the account WS, and completes
// only after a verified initial REST snapshot (equity is not on the WS) has
// been emitted.
func (e *Executor) Connect(ctx context.Context) (godex.ExecutionMetadata, error) {
	e.opMu.Lock()
	if e.closed {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, godex.ErrClosed
	}
	if e.connected {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, fmt.Errorf("lighter: executor already connected")
	}
	e.connected = true
	e.opMu.Unlock()

	metadata, err := e.connect(ctx)
	if err != nil {
		e.opMu.Lock()
		e.connected = false
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, err
	}
	return metadata, nil
}

func (e *Executor) connect(ctx context.Context) (godex.ExecutionMetadata, error) {
	e.resetObservationState()

	meta, err := e.loadMarketMeta(ctx)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.meta = meta

	sgnr, err := e.cfg.newSigner(e.cfg)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	if err := sgnr.check(); err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.signer = sgnr

	nonce := newNonceManager(func(ctx context.Context) (int64, error) {
		path := fmt.Sprintf("/api/v1/nextNonce?account_index=%d&api_key_index=%d",
			e.cfg.credentials.AccountIndex, e.cfg.credentials.APIKeyIndex)
		response, err := getJSON[nextNonceResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL, path)
		if err != nil {
			return 0, err
		}
		return *response.Nonce, nil
	})
	if err := nonce.init(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.txMu.Lock()
	e.nonce = nonce
	e.txMu.Unlock()

	e.socket = ws.New(wsLabel, e.cfg.wsURL, e.cfg.reconnect, e.logger, ws.Handlers{
		OnOpen:    e.handleSocketOpen,
		OnMessage: e.handleSocketMessage,
		OnDown:    e.handleSocketDown,
	})
	if err := e.socket.Start(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}

	if err := e.applyInitialSnapshot(ctx); err != nil {
		_ = e.socket.Stop()
		e.clearAuthRefresh()
		return godex.ExecutionMetadata{}, err
	}

	e.pollerWG.Add(2)
	go e.pingLoop()
	go e.marginPollLoop()

	e.txMu.Lock()
	e.acceptingTx = true
	e.txMu.Unlock()

	return godex.ExecutionMetadata{
		SizeStep:                  decimal.New(1, *meta.SupportedSizeDecimals),
		MaintenanceMarginFraction: decimal.New(*meta.MaintenanceMarginFraction, marginFractionScale),
	}, nil
}

func (e *Executor) resetObservationState() {
	e.stateMu.Lock()
	e.observationSeq = 0
	e.lastStateObservation = 0
	e.pendingSubscriptionSeq = 0
	e.hasPositionSnapshot = false
	e.hasMarginSnapshot = false
	e.accountInvalid = nil
	e.stateMu.Unlock()
	e.txMu.Lock()
	e.txFault = nil
	if e.faultTimer != nil {
		e.faultTimer.Stop()
		e.faultTimer = nil
	}
	e.txMu.Unlock()
}

func (e *Executor) loadMarketMeta(ctx context.Context) (*orderBookDetail, error) {
	response, err := getJSON[orderBookDetailsResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL, "/api/v1/orderBookDetails")
	if err != nil {
		return nil, err
	}
	for i := range *response.OrderBookDetails {
		detail := &(*response.OrderBookDetails)[i]
		if *detail.MarketID != e.cfg.marketID {
			continue
		}
		if *detail.Status != marketStatusActive {
			return nil, fmt.Errorf("lighter: market %d is not active: %s", e.cfg.marketID, *detail.Status)
		}
		return detail, nil
	}
	return nil, fmt.Errorf("lighter: market not found: market_id=%d", e.cfg.marketID)
}

// applyInitialSnapshot fetches the initial REST account snapshot, retrying
// when it loses the staleness race against newer WS state.
func (e *Executor) applyInitialSnapshot(ctx context.Context) error {
	applied := false
	for attempt := 0; attempt < initialSnapshotAttempts; attempt++ {
		ok, err := e.refreshAccount(ctx)
		if err != nil {
			return err
		}
		if ok {
			applied = true
			break
		}
	}
	e.stateMu.Lock()
	complete := applied && e.hasPositionSnapshot && e.hasMarginSnapshot && e.accountInvalid == nil
	e.stateMu.Unlock()
	if !complete {
		return fmt.Errorf("lighter: initial account snapshot was not fully applied")
	}
	return nil
}

// Close implements godex.VenueExecutor. It is terminal and idempotent.
func (e *Executor) Close() error {
	e.opMu.Lock()
	if e.closed {
		e.opMu.Unlock()
		return nil
	}
	e.closed = true
	e.opMu.Unlock()

	e.txMu.Lock()
	e.acceptingTx = false
	if e.faultTimer != nil {
		e.faultTimer.Stop()
		e.faultTimer = nil
	}
	e.txMu.Unlock()

	// Unblock in-flight REST calls and any emitter waiting on a full events
	// channel, then tear the socket down (its Stop delivers the final
	// DisconnectedEvent via OnDown before returning).
	e.lifecycleCancel()
	if e.socket != nil {
		_ = e.socket.Stop()
	}
	e.clearAuthRefresh()
	e.pollerWG.Wait()
	e.opWG.Wait()

	close(e.events)
	return nil
}

// ForceReconnect force-closes the current account WS connection so the
// automatic reconnect path (fresh auth token, resubscription, snapshot
// re-convergence) runs. Used by the smoke-test reconnect gate.
func (e *Executor) ForceReconnect() error {
	if e.socket == nil {
		return godex.ErrNotConnected
	}
	e.socket.Abort()
	return nil
}

// beginOp registers an in-flight operation that may emit events; Close waits
// for all of them before closing the events channel.
func (e *Executor) beginOp() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.closed {
		return godex.ErrClosed
	}
	e.opWG.Add(1)
	return nil
}

func (e *Executor) endOp() {
	e.opWG.Done()
}

// PlaceOrder implements godex.VenueExecutor.
func (e *Executor) PlaceOrder(ctx context.Context, order godex.NewOrder) (godex.OrderAck, error) {
	if err := e.beginOp(); err != nil {
		return godex.OrderAck{}, err
	}
	defer e.endOp()

	e.stateMu.Lock()
	invalid := e.accountInvalid
	e.stateMu.Unlock()
	if invalid != nil {
		return godex.OrderAck{}, fmt.Errorf("lighter: account state is invalid: %w", invalid.err)
	}
	if order.Symbol != e.cfg.symbol {
		return godex.OrderAck{}, fmt.Errorf("lighter: executor is configured for %s, got %s", e.cfg.symbol, order.Symbol)
	}
	meta := e.meta
	if meta == nil {
		return godex.OrderAck{}, godex.ErrNotConnected
	}

	tick := decimal.New(1, *meta.SupportedPriceDecimals)
	step := decimal.New(1, *meta.SupportedSizeDecimals)
	price, err := godex.RoundPriceToTick(order.Price, tick, order.Side)
	if err != nil {
		return godex.OrderAck{}, err
	}
	// min_base_amount / min_quote_amount apply to maker (post-only) orders
	// only; IOC (taker) gets step-only quantization.
	isMaker := order.Intent == godex.IntentPostOnly
	var size decimal.Decimal
	if order.ReduceOnly {
		size, err = godex.QuantizeReduceOnlySize(order.Size, step)
	} else {
		minSize := step
		if isMaker {
			minSize, err = decimal.FromDecimalString(*meta.MinBaseAmount)
			if err != nil {
				return godex.OrderAck{}, err
			}
		}
		size, err = godex.QuantizeSize(order.Size, step, minSize)
	}
	if err != nil {
		return godex.OrderAck{}, err
	}

	orderID, clientOrderIndex := e.allocateOrderID()

	if isMaker {
		// The venue rejects makers below min_quote_amount; pre-reject
		// locally as a normal-path order_rejected, mirroring post-only
		// rejections (not an error).
		minQuote, err := decimal.FromDecimalString(*meta.MinQuoteAmount)
		if err != nil {
			return godex.OrderAck{}, err
		}
		notional := price.MulToScale(size, quoteNotionalScale)
		if notional.Cmp(minQuote) < 0 {
			reason := fmt.Sprintf("order notional is below lighter min_quote_amount %s", *meta.MinQuoteAmount)
			e.emitEvent(godex.OrderRejectedEvent{OrderID: orderID, Reason: reason})
			return godex.OrderAck{OrderID: orderID, VenueID: godex.VenueLighter, Status: godex.AckRejected, Time: e.cfg.now()}, nil
		}
	}

	// Wire-integer bounds (fail fast instead of truncating).
	priceMantissa := price.Rescale(*meta.SupportedPriceDecimals).Mantissa()
	if !priceMantissa.IsUint64() || priceMantissa.Uint64() > math.MaxUint32 {
		return godex.OrderAck{}, fmt.Errorf("lighter: price %s overflows the venue's uint32 price field", price)
	}
	baseMantissa := size.Rescale(*meta.SupportedSizeDecimals).Mantissa()
	if !baseMantissa.IsInt64() || baseMantissa.Int64() > txtypes.MaxOrderBaseAmount {
		return godex.OrderAck{}, fmt.Errorf("lighter: size %s overflows the venue's base amount field", size)
	}

	orderExpiryAt := txtypes.NilOrderExpiry
	if isMaker {
		// Local resolution of the shared-lib "-1 = 28 days" sentinel.
		orderExpiryAt = e.cfg.now().Add(postOnlyOrderExpiry).UnixMilli()
	}
	params := createOrderParams{
		marketIndex:      e.cfg.marketIndex,
		clientOrderIndex: clientOrderIndex,
		baseAmount:       baseMantissa.Int64(),
		price:            uint32(priceMantissa.Uint64()),
		isAsk:            order.Side == godex.SideSell,
		postOnly:         isMaker,
		reduceOnly:       order.ReduceOnly,
		orderExpiryAt:    orderExpiryAt,
	}

	e.trackOrder(orderID, clientOrderIndex)
	failure, err := e.submitSignedTx(ctx, func(nonce int64) (uint8, string, error) {
		params.nonce = nonce
		return e.signer.signCreateOrder(params)
	})
	if err != nil {
		e.untrackOrder(orderID)
		return godex.OrderAck{}, err
	}
	if failure != "" {
		e.untrackOrder(orderID)
		if nonceErrorPattern.MatchString(failure) {
			// Nonce drift that survived serialization plus one
			// resync-and-retry; fail the order and leave resolution to the
			// strategy layer.
			return godex.OrderAck{}, fmt.Errorf("lighter: order failed on nonce mismatch (resynced): %s", failure)
		}
		if postOnlyRejectPattern.MatchString(failure) {
			e.emitEvent(godex.OrderRejectedEvent{OrderID: orderID, Reason: failure})
			return godex.OrderAck{OrderID: orderID, VenueID: godex.VenueLighter, Status: godex.AckRejected, Time: e.cfg.now()}, nil
		}
		return godex.OrderAck{}, fmt.Errorf("lighter: order placement failed: %s", failure)
	}
	return godex.OrderAck{OrderID: orderID, VenueID: godex.VenueLighter, Status: godex.AckSubmitted, Time: e.cfg.now()}, nil
}

// CancelOrder implements godex.VenueExecutor. Cancellation signs over the
// client order index recorded at submission.
func (e *Executor) CancelOrder(ctx context.Context, id godex.OrderID) error {
	if err := e.beginOp(); err != nil {
		return err
	}
	defer e.endOp()

	e.stateMu.Lock()
	clientOrderIndex, tracked := e.orders[id]
	_, alreadyCanceling := e.canceling[id]
	if tracked && !alreadyCanceling {
		// Recorded before dispatch, not after the answer: the account stream
		// can report the order gone while the cancel is still in flight, and
		// the reason that report carries must not depend on which of the two
		// lands first. An order being cancelled is also no longer
		// addressable, so a second cancel has nothing to act on.
		e.canceling[id] = struct{}{}
	}
	e.stateMu.Unlock()
	if !tracked || alreadyCanceling {
		return fmt.Errorf("%w: %s", godex.ErrUnknownOrder, id)
	}
	// A cancel that did not take leaves the order addressable again, so the
	// intent is withdrawn on every path that does not return success —
	// including an unknown outcome, because cancel-by-client-index is
	// idempotent and retrying it is how that fault recovers. The cost is that
	// a cancel which did apply after an unknown outcome is reported under the
	// venue's own wording; that is the honest answer, since the adapter never
	// learned its cancel was the cause.
	failure, err := e.submitSignedTx(ctx, func(nonce int64) (uint8, string, error) {
		return e.signer.signCancelOrder(e.cfg.marketIndex, clientOrderIndex, nonce)
	})
	if err != nil {
		e.clearCancelIntent(id)
		return err
	}
	if failure != "" {
		// Any nonce resync already happened inside submitSignedTx.
		e.clearCancelIntent(id)
		return fmt.Errorf("lighter: cancel failed: %s", failure)
	}
	// sendTx accepting the transaction is not the order having ended by it:
	// this venue accepts and then rejects asynchronously, so a cancel accepted
	// the instant the order filled applies to nothing. The order stays
	// tracked, and the account stream reports how it actually ended.
	//
	// The stream reports only post-only cancellations today, so a
	// caller-initiated cancel of a resting order is not yet reported at all;
	// see the package comment.
	return nil
}

func (e *Executor) clearCancelIntent(id godex.OrderID) {
	e.stateMu.Lock()
	delete(e.canceling, id)
	e.stateMu.Unlock()
}

func (e *Executor) allocateOrderID() (godex.OrderID, int64) {
	e.stateMu.Lock()
	clientOrderIndex := e.clientOrderCounter
	e.clientOrderCounter++
	e.stateMu.Unlock()
	return godex.OrderID(strconv.FormatInt(clientOrderIndex, 10)), clientOrderIndex
}

func (e *Executor) trackOrder(id godex.OrderID, clientOrderIndex int64) {
	e.stateMu.Lock()
	e.orders[id] = clientOrderIndex
	e.stateMu.Unlock()
}

func (e *Executor) untrackOrder(id godex.OrderID) {
	e.stateMu.Lock()
	e.untrackOrderLocked(id)
	e.stateMu.Unlock()
}

func (e *Executor) untrackOrderLocked(id godex.OrderID) {
	delete(e.orders, id)
	delete(e.canceling, id)
}

// submitSignedTx serializes take-nonce → sign → sendTx under txMu so the
// server-required strictly-increasing nonce order equals submission order.
// Returns ("", nil) on success, (message, nil) for a known API rejection
// (post-only classification is the caller's job), and ("", err) otherwise.
// Only invalid-nonce rejections are retried, once, after a resync — an
// invalid-nonce rejection is unprocessed, so the resend cannot double-submit.
func (e *Executor) submitSignedTx(ctx context.Context, sign func(nonce int64) (uint8, string, error)) (string, error) {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if !e.acceptingTx {
		return "", fmt.Errorf("lighter: executor is not accepting transactions: %w", godex.ErrNotConnected)
	}
	for attempt := 0; ; attempt++ {
		if err := e.assertTxCanStartLocked(ctx); err != nil {
			return "", err
		}
		nonce, err := e.nonce.take()
		if err != nil {
			return "", err
		}
		txType, txInfo, err := sign(nonce)
		if err != nil {
			// The allocated nonce was never submitted; resync before
			// surfacing the signing error.
			if resyncErr := e.resyncNonceLocked(); resyncErr != nil {
				return "", resyncErr
			}
			return "", err
		}
		failure, err := e.sendTxLocked(txType, txInfo)
		if err != nil {
			if e.lifecycleCtx.Err() != nil {
				return "", fmt.Errorf("lighter: transaction lifecycle ended: %w", e.lifecycleCtx.Err())
			}
			return "", e.latchTxFaultLocked(err)
		}
		if failure == "" {
			return "", nil
		}
		// API-level rejections do not advance the server nonce; restore
		// sync on every rejection path.
		if err := e.resyncNonceLocked(); err != nil {
			return "", err
		}
		if attempt < nonceRetryLimit && nonceErrorPattern.MatchString(failure) {
			continue
		}
		return failure, nil
	}
}

func (e *Executor) assertTxCanStartLocked(ctx context.Context) error {
	if err := e.lifecycleCtx.Err(); err != nil {
		return fmt.Errorf("lighter: transaction lifecycle ended: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("lighter: order submission canceled before sendTx: %w", err)
	}
	if e.txFault != nil {
		return e.txFault
	}
	return nil
}

func (e *Executor) sendTxLocked(txType uint8, txInfo string) (string, error) {
	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	return sendTx(requestCtx, e.cfg.httpClient, e.cfg.restBaseURL, txType, txInfo)
}

func (e *Executor) resyncNonceLocked() error {
	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	if err := e.nonce.resync(requestCtx); err != nil {
		if e.lifecycleCtx.Err() != nil {
			return fmt.Errorf("lighter: transaction lifecycle ended: %w", e.lifecycleCtx.Err())
		}
		return e.latchTxFaultLocked(err)
	}
	return nil
}

// latchTxFaultLocked records an unknown-outcome fault: subsequent
// transactions are halted (no blind retries that could double-submit) and
// automatic recovery via nonce resync is scheduled. The faulted transaction
// itself is never resent.
func (e *Executor) latchTxFaultLocked(cause error) error {
	if e.txFault == nil {
		e.txFault = fmt.Errorf("%w; recovering via nonce resync: %v", godex.ErrTxOutcomeUnknown, cause)
		e.scheduleFaultRecoveryLocked()
	}
	return e.txFault
}

func (e *Executor) scheduleFaultRecoveryLocked() {
	if e.faultTimer != nil || !e.acceptingTx {
		return
	}
	e.faultTimer = time.AfterFunc(e.cfg.txFaultRecoveryDelay, e.recoverTxFault)
}

// recoverTxFault resyncs the nonce under the transaction mutex. The resync
// fetches the server's true next nonce, so consistency recovers whether or
// not the unknown-outcome transaction landed; clearing the fault is safe
// because the faulted transaction is never resent. Unreachable endpoints
// reschedule with backoff.
func (e *Executor) recoverTxFault() {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	e.faultTimer = nil
	if e.txFault == nil || !e.acceptingTx || e.lifecycleCtx.Err() != nil {
		return
	}
	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	err := e.nonce.resync(requestCtx)
	cancel()
	if err != nil {
		if e.acceptingTx && e.lifecycleCtx.Err() == nil {
			e.scheduleFaultRecoveryLocked()
		}
		return
	}
	e.txFault = nil
	e.logger.Info("lighter tx fault recovered via nonce resync")
}

// --- account stream ---

func (e *Executor) handleSocketOpen() error {
	e.stateMu.Lock()
	e.pendingSubscriptionSeq = e.nextObservationSeqLocked()
	e.stateMu.Unlock()

	// A fresh auth token per (re)connection (TTL ~10 minutes).
	deadline := e.cfg.now().Add(authTokenTTL)
	auth, err := e.signer.createAuthToken(deadline)
	if err != nil {
		return err
	}
	// account_all: positions / trades (fills); margin arrives via REST polls.
	if err := e.sendSubscribe("account_all", auth); err != nil {
		return err
	}
	// account_all_orders: order status ("canceled-post-only" arrives here).
	if err := e.sendSubscribe("account_all_orders", auth); err != nil {
		return err
	}
	e.scheduleAuthRefresh()
	e.emitEvent(godex.ConnectedEvent{VenueID: godex.VenueLighter})
	return nil
}

func (e *Executor) sendSubscribe(channel string, auth string) error {
	message, err := json.Marshal(map[string]string{
		"type":    "subscribe",
		"channel": fmt.Sprintf("%s/%d", channel, e.cfg.credentials.AccountIndex),
		"auth":    auth,
	})
	if err != nil {
		return err
	}
	return e.socket.Send(string(message))
}

func (e *Executor) handleSocketDown() {
	e.clearAuthRefresh()
	e.emitEvent(godex.DisconnectedEvent{VenueID: godex.VenueLighter})
}

func (e *Executor) handleSocketMessage(raw []byte) error {
	message, err := decodeAccountWsMessage(raw)
	if err != nil {
		return err
	}
	switch message.Type {
	case wsTypePing:
		// Reply to server-initiated pings (as the reference ws client does).
		return e.socket.Send(pongMessage)
	case wsTypeConnected, wsTypePong:
		return nil
	}

	// The subscription snapshot reuses the sequence reserved at open so a
	// REST snapshot fetched in between cannot be mistaken for newer state.
	e.stateMu.Lock()
	var sequence int64
	if message.Type == wsTypeSubscribedAccountAll && e.pendingSubscriptionSeq != 0 {
		sequence = e.pendingSubscriptionSeq
		e.pendingSubscriptionSeq = 0
	} else {
		sequence = e.nextObservationSeqLocked()
	}
	e.stateMu.Unlock()

	result, err := normalizeAccountUpdate(message.Payload, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return err
	}
	if result.accountErr != nil {
		e.setAccountInvalid(result.accountErr, sequence)
	}
	e.applyStateEvents(result.events, sequence)
	if result.accountErr != nil {
		// Events were applied; now abort the connection (venue isolation —
		// the process stays up, PlaceOrder is blocked by the invalid latch).
		return result.accountErr
	}
	if result.needsAccountRefresh {
		if e.beginOp() == nil {
			go func() {
				defer e.endOp()
				if _, err := e.refreshAccount(e.lifecycleCtx); err != nil && e.lifecycleCtx.Err() == nil {
					e.logger.Error("lighter account refresh failed", "error", err)
				}
			}()
		}
	}
	return nil
}

// refreshAccount fetches the REST account state and emits position + margin
// events. The observation sequence is reserved before the fetch starts so a
// slow response cannot overwrite newer WS state.
func (e *Executor) refreshAccount(ctx context.Context) (bool, error) {
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	e.stateMu.Unlock()

	path := fmt.Sprintf("/api/v1/account?by=index&value=%d", e.cfg.credentials.AccountIndex)
	response, err := getJSON[accountRestResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL, path)
	if err != nil {
		return false, err
	}
	account := &(*response.Accounts)[0]
	result, err := normalizeAccount(account, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return false, err
	}
	if result.needsAccountRefresh {
		err := fmt.Errorf("lighter: REST account position is incomplete")
		e.setAccountInvalid(err, sequence)
		return false, err
	}
	applied := e.applyStateEvents(result.events, sequence)
	e.clearAccountInvalid(sequence, applied)
	return applied, nil
}

// applyStateEvents emits a normalization batch, dropping stale position and
// margin observations (fills and rejections are never dropped). Reports
// whether fresh state was applied.
func (e *Executor) applyStateEvents(events []godex.AccountEvent, sequence int64) bool {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	hasStateEvent := false
	for _, event := range events {
		switch event.(type) {
		case godex.PositionEvent, godex.MarginEvent:
			hasStateEvent = true
		}
	}
	staleState := hasStateEvent &&
		(sequence < e.lastStateObservation ||
			(e.accountInvalid != nil && sequence <= e.accountInvalid.sequence))
	if hasStateEvent && !staleState {
		e.lastStateObservation = sequence
	}
	for _, event := range events {
		switch typed := event.(type) {
		case godex.PositionEvent:
			if staleState {
				continue
			}
			e.hasPositionSnapshot = true
		case godex.MarginEvent:
			if staleState {
				continue
			}
			e.hasMarginSnapshot = true
		case godex.OrderRejectedEvent:
			// The order is finished, so it stops being tracked here rather
			// than being remembered for the process's lifetime. A cancel the
			// caller asked for is what the venue's own wording is replaced
			// by, so the reason does not depend on which report arrived first.
			event = godex.OrderRejectedEvent{
				OrderID: typed.OrderID,
				Reason:  e.terminalReasonLocked(typed.OrderID, typed.Reason),
			}
			e.untrackOrderLocked(typed.OrderID)
		}
		e.send(event)
	}
	return hasStateEvent && !staleState
}

// terminalReasonLocked names why an order ended. A cancel the caller asked for
// and the venue accepted reads the same on every venue, so it wins over the
// venue's own wording for the removal it caused.
func (e *Executor) terminalReasonLocked(id godex.OrderID, venueReason string) string {
	if _, requested := e.canceling[id]; requested {
		return godex.ReasonCanceledByRequest
	}
	return venueReason
}

func (e *Executor) setAccountInvalid(err error, sequence int64) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if sequence <= e.lastStateObservation ||
		(e.accountInvalid != nil && sequence <= e.accountInvalid.sequence) {
		return
	}
	e.accountInvalid = &accountInvalidObservation{err: err, sequence: sequence}
}

func (e *Executor) clearAccountInvalid(sequence int64, stateApplied bool) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if stateApplied && e.accountInvalid != nil && sequence > e.accountInvalid.sequence {
		e.accountInvalid = nil
	}
}

func (e *Executor) nextObservationSeqLocked() int64 {
	e.observationSeq++
	return e.observationSeq
}

func (e *Executor) normalizeContext() normalizeContext {
	return normalizeContext{
		symbol:       e.cfg.symbol,
		marketID:     e.cfg.marketID,
		accountIndex: e.cfg.credentials.AccountIndex,
		receivedAt:   e.cfg.now(),
	}
}

// --- timers and pollers ---

func (e *Executor) pingLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if e.socket.IsOpen() {
				// A lost race with a concurrent disconnect is harmless.
				_ = e.socket.Send(pingMessage)
			}
		}
	}
}

func (e *Executor) marginPollLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(marginPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if !e.socket.IsOpen() {
				continue
			}
			// Poll failures are transient I/O; freshness monitoring is the
			// risk layer's responsibility.
			if _, err := e.refreshAccount(e.lifecycleCtx); err != nil && e.lifecycleCtx.Err() == nil {
				e.logger.Error("lighter margin poll failed", "error", err)
			}
		}
	}
}

// scheduleAuthRefresh rebuilds the connection shortly before the auth token
// expires so subscriptions continue under a fresh token (post-expiry behavior
// is unverified upstream; stay conservative).
func (e *Executor) scheduleAuthRefresh() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.authRefreshTimer != nil {
		e.authRefreshTimer.Stop()
	}
	e.authRefreshTimer = time.AfterFunc(authTokenTTL-authRefreshMargin, func() {
		e.logger.Warn("lighter auth token refresh — reconnecting account ws")
		e.socket.Abort()
	})
}

func (e *Executor) clearAuthRefresh() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.authRefreshTimer != nil {
		e.authRefreshTimer.Stop()
		e.authRefreshTimer = nil
	}
}

// --- event emission ---

// send delivers an event; callers hold stateMu so batches stay ordered. A
// full buffer blocks (dropping a fill would silently corrupt position state)
// until the consumer drains or the executor closes. The non-blocking first
// attempt guarantees delivery whenever the buffer has room — in particular
// the final DisconnectedEvent during Close, which runs with the lifecycle
// context already canceled.
//
// One order's rejection is reported at most once. Two paths observe the same
// outcome — sendTx's own answer to the submission and the account stream's
// order status — and neither is ordered against the other, so the losing path
// is dropped here rather than at each call site. This also absorbs the order
// statuses an account snapshot replays after a reconnect. The dropped copy
// carries no news: OrderRejectedEvent means the order is finished, which is
// not a fact that can arrive twice with different content.
func (e *Executor) send(event godex.AccountEvent) {
	if rejection, ok := event.(godex.OrderRejectedEvent); ok {
		if !e.rejections.Observe(rejection.OrderID) {
			return
		}
	}
	select {
	case e.events <- event:
		return
	default:
	}
	select {
	case e.events <- event:
	case <-e.lifecycleCtx.Done():
	}
}

// emitEvent delivers a single event outside a normalization batch.
func (e *Executor) emitEvent(event godex.AccountEvent) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.send(event)
}
