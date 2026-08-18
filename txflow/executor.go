// Package txflow implements godex.VenueExecutor for TxFlow. It signs limit
// orders (post-only / IOC) with a trading (agent) wallet and submits them to
// the /exchange endpoint, observing the account through the orderUpdates
// stream, clearinghouse snapshots, and the userFills query.
//
// TxFlow's API descends from Hyperliquid's, with differences this package
// absorbs: markets are keyed by an explicit asset index, the info endpoint
// allowlists its query types (no meta, orderStatus, or fills stream), the
// signing domain and Agent struct are wider, and — the one that shapes the
// executor — fills are not streamed. Executions are read by polling the
// userFills query, deduplicated by trade id, and attributed to orders by the
// venue oid the placing response returned.
//
// Known limitation: an order whose placing response was lost (an unknown
// outcome) has no venue oid, so it is recovered by matching the account's
// order history — market, side, price, size, reduce-only, at or after the
// submission time — rather than by a client order id; the venue's own client
// sends no client id and whether the venue accepts one is unverified. An
// identical order placed on the same account by another process inside that
// window is indistinguishable and would be claimed and cancelled, so an
// account this executor trades should not be traded by anything else at the
// same time.
package txflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/internal/dedupe"
	"github.com/DaisukeYoda/godex/internal/evmsign"
	"github.com/DaisukeYoda/godex/internal/ws"
)

const wsLabel = "txflow-account"

// fillCacheCapacity bounds the remembered trade ids. Every userFills poll
// replays the account's recent executions (a few hundred), so a cache many
// times that size cannot evict an id that is still being replayed.
const fillCacheCapacity = 8192

// recoveryClockSlack widens the submission-time window an unknown-outcome
// recovery searches, so a venue clock slightly behind this process cannot
// hide the order being looked for.
const recoveryClockSlack = 5 * time.Second

type accountInvalidObservation struct {
	err      error
	sequence int64
}

// submission is what an order looked like when it was dispatched.
type submission struct {
	wire orderWire
	at   time.Time
}

// Executor is the TxFlow implementation of godex.VenueExecutor.
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

	// txMu serializes allocate-nonce → sign → submit so nonce order equals
	// submission order (the venue requires increasing nonces per wallet). It
	// also guards the fault latch.
	txMu           sync.Mutex
	lastNonce      uint64
	txFault        error
	txFaultOrderID godex.OrderID
	faultTimer     *time.Timer
	acceptingTx    bool

	signer signer
	socket *ws.Socket
	asset  assetMeta

	// stateMu guards account-observation bookkeeping, order tracking, and
	// event emission (holding it through emission preserves per-batch event
	// ordering).
	stateMu              sync.Mutex
	observationSeq       int64
	lastStateObservation int64
	hasPositionSnapshot  bool
	hasMarginSnapshot    bool
	accountInvalid       *accountInvalidObservation
	// orders maps the executor's order ids to venue oids (0 = not yet
	// known); submissions remembers what each was and when it was
	// dispatched, which is the only handle an unknown outcome leaves behind.
	orders      map[godex.OrderID]int64
	submissions map[godex.OrderID]submission
	// oids attributes fills, which arrive by poll and possibly after the
	// order they belong to has ended, so it outlives order tracking.
	oids *oidIndex
	// canceling holds orders whose cancel the venue accepted. They stay
	// tracked, because only the account stream (or reconciliation) can say
	// how the order actually ended — a cancel accepted the instant the order
	// filled applies to nothing. Membership is what makes a second cancel
	// unaddressable and what labels the terminal event when it arrives.
	canceling map[godex.OrderID]struct{}
	// orphanStatuses remembers order updates for oids no order was bound to
	// yet. The placing response and the order's first update race, and an
	// update that wins — an IOC filled in the same block, say — must not be
	// lost: it is applied the moment the oid is bound.
	orphanStatuses map[int64]string
	fills          *dedupe.Set[int64]
	// fillHistorySeeded records that the first userFills read has been
	// absorbed. That read is the account's history, not this executor's
	// work, so it seeds the dedupe cache without being published; every later
	// poll publishes whatever it carries that the cache has not seen.
	fillHistorySeeded bool
	// connGeneration counts connections; connOpen tracks whether the current
	// one is up. Account and fill reads run off the socket, so a slow
	// response can land after its connection dropped — the pair is what keeps
	// those results from being published outside a Connected/Disconnected
	// window.
	connGeneration int
	connOpen       bool

	// fillPollTrigger wakes the fill poller ahead of its tick, after an
	// event that makes a fill likely (a filled order update, a reconnect).
	fillPollTrigger chan struct{}
	pollerWG        sync.WaitGroup
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
		submissions:     make(map[godex.OrderID]submission),
		oids:            newOidIndex(),
		canceling:       make(map[godex.OrderID]struct{}),
		orphanStatuses:  make(map[int64]string),
		fills:           dedupe.NewSet[int64](fillCacheCapacity),
		fillPollTrigger: make(chan struct{}, 1),
	}, nil
}

// VenueID implements godex.VenueExecutor.
func (e *Executor) VenueID() godex.VenueID {
	return godex.VenueTxFlow
}

// AccountEvents implements godex.VenueExecutor.
func (e *Executor) AccountEvents() <-chan godex.AccountEvent {
	return e.events
}

// Connect implements godex.VenueExecutor: it resolves the perp's asset id and
// quantization, builds the signer, absorbs the account's fill history, starts
// the account stream, and completes only after a verified clearinghouse
// snapshot has been emitted.
func (e *Executor) Connect(ctx context.Context) (godex.ExecutionMetadata, error) {
	e.opMu.Lock()
	if e.closed {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, godex.ErrClosed
	}
	if e.connected {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, fmt.Errorf("txflow: executor already connected")
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

	asset, err := e.loadAssetMeta(ctx)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.asset = asset

	sgnr, err := e.cfg.newSigner(e.cfg)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.signer = sgnr
	e.logger.Info("txflow signer ready",
		"agent_address", sgnr.address(), "account", e.cfg.accountAddress, "market", e.cfg.market)

	// Margin mode is checked before the socket opens: an order action does
	// not carry one, so an account left in isolated mode would silently open
	// a position the adapter's whole-account liquidation math cannot describe.
	if err := e.assertCrossMargin(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}

	// The account's fill history is absorbed before any order can be placed:
	// until then the executor cannot tell history from its own executions,
	// and accepting an order first would risk suppressing its fill.
	if err := e.seedFillHistory(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}

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
		return godex.ExecutionMetadata{}, err
	}

	e.pollerWG.Add(3)
	go e.pingLoop()
	go e.accountPollLoop()
	go e.fillPollLoop()

	e.txMu.Lock()
	e.acceptingTx = true
	e.txMu.Unlock()

	return godex.ExecutionMetadata{
		SizeStep:                  asset.sizeStep,
		MaintenanceMarginFraction: e.cfg.marginFraction,
	}, nil
}

func (e *Executor) resetObservationState() {
	e.stateMu.Lock()
	e.observationSeq = 0
	e.lastStateObservation = 0
	e.hasPositionSnapshot = false
	e.hasMarginSnapshot = false
	e.accountInvalid = nil
	e.stateMu.Unlock()
	e.txMu.Lock()
	e.txFault = nil
	e.txFaultOrderID = ""
	if e.faultTimer != nil {
		e.faultTimer.Stop()
		e.faultTimer = nil
	}
	e.txMu.Unlock()
}

// loadAssetMeta resolves the configured market to its asset id and
// quantization.
func (e *Executor) loadAssetMeta(ctx context.Context) (assetMeta, error) {
	response, err := postJSON[perpMetaResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypePerpMeta})
	if err != nil {
		return assetMeta{}, err
	}
	return resolveAssetMeta(response, e.cfg.market)
}

// coinParam is the market's asset index in the form per-market info queries
// take.
func (e *Executor) coinParam() string {
	return strconv.Itoa(e.asset.index)
}

// applyInitialSnapshot fetches the initial clearinghouse snapshot, retrying
// when the venue reports a position it cannot be in (size at no price).
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
		return fmt.Errorf("txflow: initial account snapshot was not fully applied")
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

	// Unblock in-flight REST calls — including a submission holding txMu —
	// and any emitter waiting on a full events channel, then tear the socket
	// down (its Stop delivers the final DisconnectedEvent via OnDown before
	// returning).
	e.lifecycleCancel()

	e.txMu.Lock()
	e.acceptingTx = false
	if e.faultTimer != nil {
		e.faultTimer.Stop()
		e.faultTimer = nil
	}
	e.txMu.Unlock()

	if e.socket != nil {
		_ = e.socket.Stop()
	}
	e.pollerWG.Wait()
	e.opWG.Wait()

	close(e.events)
	return nil
}

// ForceReconnect force-closes the current account WS connection so the
// automatic reconnect path (resubscription, snapshot re-convergence) runs.
// Used by the smoke-test reconnect gate.
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

// --- order flow ---

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
		return godex.OrderAck{}, fmt.Errorf("txflow: account state is invalid: %w", invalid.err)
	}
	if order.Symbol != e.cfg.symbol {
		return godex.OrderAck{}, fmt.Errorf("txflow: executor is configured for %s, got %s", e.cfg.symbol, order.Symbol)
	}
	if e.signer == nil {
		return godex.OrderAck{}, godex.ErrNotConnected
	}

	price, err := godex.RoundPriceToTick(order.Price, e.asset.priceTick, order.Side)
	if err != nil {
		return godex.OrderAck{}, err
	}
	var size decimal.Decimal
	if order.ReduceOnly {
		size, err = godex.QuantizeReduceOnlySize(order.Size, e.asset.sizeStep)
	} else {
		size, err = godex.QuantizeSize(order.Size, e.asset.sizeStep, e.asset.sizeStep)
	}
	if err != nil {
		return godex.OrderAck{}, err
	}

	tif := tifIOC
	if order.Intent == godex.IntentPostOnly {
		tif = tifALO
	} else if order.Intent != godex.IntentIOC {
		return godex.OrderAck{}, fmt.Errorf("txflow: unsupported order intent %q", order.Intent)
	}

	orderID, err := newClientOrderID()
	if err != nil {
		return godex.OrderAck{}, err
	}
	action := orderAction{
		Type:     actionTypeOrder,
		Grouping: groupingNA,
		Orders: []orderWire{{
			Asset:      e.asset.index,
			IsBuy:      order.Side == godex.SideBuy,
			Price:      evmsign.WireDecimal(price),
			Size:       evmsign.WireDecimal(size),
			ReduceOnly: order.ReduceOnly,
			OrderType:  orderTypeWire{Limit: limitOrderWire{Tif: tif}},
		}},
	}

	// Track before submitting: if the outcome turns out to be unknown, the
	// order and its submission time must already be on record so
	// reconciliation can look for it.
	e.trackOrder(orderID, action.Orders[0])
	statuses, failure, err := e.submitAction(ctx, action, orderID)
	if err != nil {
		// An order left in flight may be live, so it stays tracked until
		// reconciliation settles it. Anything else — including a submission
		// the fault latch refused to dispatch — never reached the venue.
		if !e.isAmbiguousSubmission(orderID) {
			e.untrackOrder(orderID)
		}
		return godex.OrderAck{}, err
	}
	if failure != "" {
		e.untrackOrder(orderID)
		return godex.OrderAck{}, fmt.Errorf("txflow: order placement failed: %s", failure)
	}

	status, err := decodeOrderStatus(statuses)
	if err != nil {
		// The venue answered "ok", so it may well be holding a resting
		// order; an outcome that cannot be read is unknown, not failed.
		return godex.OrderAck{}, e.latchTxFault(err, orderID)
	}
	switch {
	case status.Error != nil:
		e.untrackOrder(orderID)
		if postOnlyRejectPattern.MatchString(*status.Error) {
			e.emitEvent(godex.OrderRejectedEvent{OrderID: orderID, Reason: *status.Error})
			return godex.OrderAck{
				OrderID: orderID, VenueID: godex.VenueTxFlow,
				Status: godex.AckRejected, Time: e.cfg.now(),
			}, nil
		}
		return godex.OrderAck{}, fmt.Errorf("txflow: order rejected: %s", *status.Error)
	case status.Resting != nil:
		e.bindOrderOid(orderID, *status.Resting.Oid)
	case status.Filled != nil:
		// The execution itself is reported from the userFills query, which
		// is the only source of truth for fills; the oid is bound so it can
		// be attributed, and the poller is woken so it lands promptly.
		e.bindOrderOid(orderID, *status.Filled.Oid)
		e.triggerFillPoll()
	default:
		return godex.OrderAck{}, e.latchTxFault(
			fmt.Errorf("txflow: order status carried no recognized outcome"), orderID)
	}
	return godex.OrderAck{
		OrderID: orderID, VenueID: godex.VenueTxFlow,
		Status: godex.AckSubmitted, Time: e.cfg.now(),
	}, nil
}

// CancelOrder implements godex.VenueExecutor. Cancellation is keyed by the
// venue oid the placing response returned.
//
// A venue answer of "never placed, already canceled, or filled" is reported
// as success: the cancel's purpose already holds, which is what makes a
// retried cancel safe after an ambiguous first attempt.
func (e *Executor) CancelOrder(ctx context.Context, id godex.OrderID) error {
	if err := e.beginOp(); err != nil {
		return err
	}
	defer e.endOp()

	e.stateMu.Lock()
	oid, tracked := e.orders[id]
	_, alreadyCanceling := e.canceling[id]
	if tracked && !alreadyCanceling && oid != 0 {
		// Recorded before dispatch, not after the answer: the account stream
		// can report the order gone while the cancel is still in flight, and
		// the reason that report carries must not depend on which of the two
		// lands first. An order being cancelled is also no longer addressable,
		// so a second cancel has nothing to act on.
		e.canceling[id] = struct{}{}
	}
	e.stateMu.Unlock()
	if !tracked || alreadyCanceling {
		return fmt.Errorf("%w: %s", godex.ErrUnknownOrder, id)
	}
	if oid == 0 {
		// Only an order whose placing outcome is unknown lacks an oid, and
		// its caller never received this id; recovery is what settles it.
		return fmt.Errorf("txflow: order %s has no venue id yet: %w", id, godex.ErrTxOutcomeUnknown)
	}

	action := cancelAction{
		Type:    actionTypeCancel,
		Cancels: []cancelWire{{Asset: e.asset.index, Oid: oid}},
	}
	// A cancel that did not take leaves the order addressable again, so the
	// intent is withdrawn on every path that does not return success —
	// including an unknown outcome, because cancelling by oid is idempotent
	// and retrying it is how that fault recovers. The cost is that a cancel
	// which did apply after an unknown outcome is reported under the venue's
	// own wording; that is the honest answer, since the adapter never learned
	// its cancel was the cause.
	statuses, failure, err := e.submitAction(ctx, action, id)
	if err != nil {
		e.clearCancelIntent(id)
		return err
	}
	if failure != "" {
		e.clearCancelIntent(id)
		return fmt.Errorf("txflow: cancel failed: %s", failure)
	}
	message, err := decodeCancelStatus(statuses)
	if err != nil {
		// The cancel may or may not have been applied; that is exactly the
		// ambiguity the fault latch exists for.
		e.clearCancelIntent(id)
		return e.latchTxFault(err, id)
	}
	if message != "" {
		if cancelAlreadyGonePattern.MatchString(message) {
			// "never placed, already canceled, or filled" does not say which.
			// Untracking on it would drop the orderUpdates push that does say,
			// leaving an order that ended with no event at all, so the venue is
			// asked outright.
			e.resolveGoneOrder(ctx, id)
			return nil
		}
		e.clearCancelIntent(id)
		return fmt.Errorf("txflow: cancel failed: %s", message)
	}
	// The venue accepted the cancel, which is not the same as the order having
	// ended by it: a cancel accepted the instant the order filled applies to
	// nothing. The order stays tracked, and the account stream reports how it
	// actually ended.
	return nil
}

func (e *Executor) clearCancelIntent(id godex.OrderID) {
	e.stateMu.Lock()
	delete(e.canceling, id)
	e.stateMu.Unlock()
}

// resolveGoneOrder settles an order the venue says it no longer holds, so the
// order is retired under the reason the venue gives rather than silently. An
// order that turns out to have filled is retired without a rejection.
func (e *Executor) resolveGoneOrder(ctx context.Context, id godex.OrderID) {
	book, err := e.readOrderBook(ctx)
	if err != nil {
		e.logger.Warn("txflow could not resolve a cancelled order's outcome; "+
			"it stays tracked for reconciliation", "order_id", id, "error", err)
		return
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	oid, still := e.orders[id]
	if !still {
		return
	}
	// The venue had nothing to cancel, so the caller's cancel is not how this
	// order ended and must not be what it is reported under.
	delete(e.canceling, id)
	e.settleOrderLocked(id, book.liveness(oid))
}

// submitAction serializes allocate-nonce → sign → POST under txMu so the
// venue-required increasing nonce order equals submission order. Returns
// (statuses, "", nil) when the venue processed the submission,
// (nil, message, nil) when it refused the whole request, and (nil, "", err)
// otherwise — an unknown outcome latches a fault.
func (e *Executor) submitAction(ctx context.Context, action any, orderID godex.OrderID) ([]json.RawMessage, string, error) {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if !e.acceptingTx {
		return nil, "", fmt.Errorf("txflow: executor is not accepting submissions: %w", godex.ErrNotConnected)
	}
	if err := e.assertTxCanStartLocked(ctx); err != nil {
		return nil, "", err
	}

	nonce := e.nextNonceLocked()
	sig, err := e.signer.signAction(action, nonce)
	if err != nil {
		return nil, "", err
	}
	request := exchangeRequest{Action: action, Nonce: nonce, Signature: sig}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	statuses, failure, err := postExchange(requestCtx, e.cfg.httpClient, e.cfg.restBaseURL, request)
	if err != nil {
		// Once dispatched, the venue may have taken the action whatever cut
		// the call short — a Close included. That is an unknown outcome and
		// is reported as one; a fault latched during Close is never recovered
		// (nothing runs after Close), which is why the caller must hear it.
		return nil, "", e.latchTxFaultLocked(err, orderID)
	}
	return statuses, failure, nil
}

func (e *Executor) assertTxCanStartLocked(ctx context.Context) error {
	if err := e.lifecycleCtx.Err(); err != nil {
		return fmt.Errorf("txflow: submission lifecycle ended: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("txflow: submission canceled before dispatch: %w", err)
	}
	if e.txFault != nil {
		return e.txFault
	}
	return nil
}

// nextNonceLocked returns a strictly increasing millisecond nonce. Wall clock
// is the venue's expected source, but two submissions inside one millisecond
// (or a clock that steps backwards) must still be ordered, so the counter
// never returns a value it has already issued.
func (e *Executor) nextNonceLocked() uint64 {
	nonce := uint64(e.cfg.now().UnixMilli())
	if nonce <= e.lastNonce {
		nonce = e.lastNonce + 1
	}
	e.lastNonce = nonce
	return nonce
}

// latchTxFaultLocked records an unknown-outcome fault: subsequent submissions
// are halted (no blind retry that could double-submit) and reconciliation is
// scheduled. The faulted submission itself is never resent.
func (e *Executor) latchTxFaultLocked(cause error, orderID godex.OrderID) error {
	if e.txFault == nil {
		e.txFault = fmt.Errorf("%w; reconciling with venue order state: %v", godex.ErrTxOutcomeUnknown, cause)
		e.txFaultOrderID = orderID
		e.scheduleFaultRecoveryLocked()
	}
	return e.txFault
}

// latchTxFault records an unknown outcome discovered after the submission
// returned — an answer the adapter could not read.
func (e *Executor) latchTxFault(cause error, orderID godex.OrderID) error {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	return e.latchTxFaultLocked(cause, orderID)
}

// isAmbiguousSubmission reports whether orderID names the submission whose
// outcome is unresolved. It is the only order a failed call may have left
// live: every other failure — signing, or a fault latch that refused to
// dispatch — happened before anything reached the venue.
func (e *Executor) isAmbiguousSubmission(orderID godex.OrderID) bool {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	return e.txFault != nil && e.txFaultOrderID == orderID
}

func (e *Executor) scheduleFaultRecoveryLocked() {
	if e.faultTimer != nil || !e.acceptingTx {
		return
	}
	e.faultTimer = time.AfterFunc(e.cfg.txFaultRecoveryDelay, e.recoverTxFault)
}

// recoverTxFault settles the ambiguous submission from the venue's order
// records. That answer — and not a retry — is what resolves the ambiguity: an
// order the venue never took is untracked, one it holds is cancelled. An
// order whose oid is already known is looked up by it; one that never got an
// ack is looked for by what was submitted — market, side, price, size,
// reduce-only — at or after the submission time, and if that search cannot
// name exactly one order the fault stays latched: guessing here could cancel
// someone else's order or leave this one resting. Unreachable endpoints
// reschedule with backoff.
func (e *Executor) recoverTxFault() {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	e.faultTimer = nil
	if e.txFault == nil || !e.acceptingTx || e.lifecycleCtx.Err() != nil {
		return
	}
	reschedule := func() {
		if e.acceptingTx && e.lifecycleCtx.Err() == nil {
			e.scheduleFaultRecoveryLocked()
		}
	}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	orderID := e.txFaultOrderID
	if orderID != "" {
		book, err := e.readOrderBook(requestCtx)
		if err != nil {
			reschedule()
			return
		}
		e.stateMu.Lock()
		oid, tracked := e.orders[orderID]
		submitted := e.submissions[orderID]
		if tracked && oid == 0 {
			var found bool
			oid, found, err = book.findSubmission(e.cfg.market, submitted, e.oids)
			if err != nil {
				e.stateMu.Unlock()
				e.logger.Error("txflow cannot attribute the submission left by an unknown outcome; "+
					"submissions stay halted", "order_id", orderID, "error", err)
				reschedule()
				return
			}
			if found {
				e.orders[orderID] = oid
				e.oids.bind(oid, orderID)
			}
		}
		e.stateMu.Unlock()

		held := tracked && oid != 0 && book.liveness(oid).state == orderLive
		if held {
			// The caller never received an ack, so it does not know this
			// order's id and cannot cancel it. Leaving it resting would be an
			// exposure nobody can address; cancelling makes the venue agree
			// with what the caller already believes. If it filled first, the
			// fill poll still reports that — cancelling is idempotent.
			if err := e.cancelForRecoveryLocked(requestCtx, oid); err != nil {
				e.logger.Error("txflow could not cancel the order left by an unknown outcome",
					"order_id", orderID, "oid", oid, "error", err)
				reschedule()
				return
			}
		}
		e.untrackOrder(orderID)
		e.logger.Info("txflow submission reconciled",
			"order_id", orderID, "venue_held_order", held)
	} else if _, err := e.readAccount(requestCtx); err != nil {
		reschedule()
		return
	}
	e.txFault = nil
	e.txFaultOrderID = ""
}

// cancelForRecoveryLocked cancels an order the venue turned out to be holding
// after an unknown outcome. It bypasses the fault latch deliberately: the
// latch exists to stop *new* exposure, and this is the call that removes the
// exposure already there. Callers hold txMu.
func (e *Executor) cancelForRecoveryLocked(ctx context.Context, oid int64) error {
	action := cancelAction{
		Type:    actionTypeCancel,
		Cancels: []cancelWire{{Asset: e.asset.index, Oid: oid}},
	}
	nonce := e.nextNonceLocked()
	sig, err := e.signer.signAction(action, nonce)
	if err != nil {
		return err
	}
	request := exchangeRequest{Action: action, Nonce: nonce, Signature: sig}
	statuses, failure, err := postExchange(ctx, e.cfg.httpClient, e.cfg.restBaseURL, request)
	if err != nil {
		return err
	}
	if failure != "" {
		return fmt.Errorf("txflow: recovery cancel refused: %s", failure)
	}
	message, err := decodeCancelStatus(statuses)
	if err != nil {
		return err
	}
	if message != "" && !cancelAlreadyGonePattern.MatchString(message) {
		return fmt.Errorf("txflow: recovery cancel failed: %s", message)
	}
	return nil
}

// --- order reconciliation ---

// orderLiveness is what the venue's records say about a tracked order.
type orderLiveness struct {
	state  livenessState
	reason string
}

type livenessState int

const (
	// orderLive means the venue still holds the order on the book.
	orderLive livenessState = iota
	// orderFilledOut means it finished by filling, which closes it without
	// being a rejection.
	orderFilledOut
	// orderClosed means it ended without filling in full.
	orderClosed
)

// venueOrderBook is one reading of the venue's order records: which orders
// rest on the book, and the last known status of every recent order. The
// venue answers no per-order status query, so this pair is how a tracked
// order's fate is looked up.
type venueOrderBook struct {
	open    map[int64]struct{}
	history map[int64]*historicalOrderWire
}

func (e *Executor) readOrderBook(ctx context.Context) (*venueOrderBook, error) {
	open, err := postJSON[openOrderList](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeOpenOrders, User: e.cfg.accountAddress})
	if err != nil {
		return nil, err
	}
	history, err := postJSON[historicalOrderList](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeHistoricalOrders, User: e.cfg.accountAddress})
	if err != nil {
		return nil, err
	}
	book := &venueOrderBook{
		open:    make(map[int64]struct{}, len(*open)),
		history: make(map[int64]*historicalOrderWire, len(*history)),
	}
	for _, order := range *open {
		book.open[*order.Oid] = struct{}{}
	}
	for i := range *history {
		order := (*history)[i].Order
		book.history[*order.Oid] = order
	}
	return book, nil
}

// liveness classifies one oid. An order on the open list is live whatever
// its history says; one that is neither open nor in the history is closed —
// the venue holds no record to cancel or fill.
func (b *venueOrderBook) liveness(oid int64) orderLiveness {
	if _, open := b.open[oid]; open {
		return orderLiveness{state: orderLive}
	}
	order, known := b.history[oid]
	if !known {
		return orderLiveness{state: orderClosed, reason: "the venue does not hold this order"}
	}
	switch status := *order.Status; status {
	case orderStatusOpen, orderStatusPartialFilled, orderStatusTriggered:
		return orderLiveness{state: orderLive}
	case orderStatusFilled:
		return orderLiveness{state: orderFilledOut}
	default:
		return orderLiveness{state: orderClosed, reason: status}
	}
}

// findSubmission looks for the one order in the records that no tracked order
// accounts for and that matches what was submitted — market, side, price,
// size, reduce-only — at or after the submission time (widened by a clock
// slack). It reports (oid, true, nil) for exactly one such order, (0, false,
// nil) for none, and an error when several match, since none of them can
// safely be claimed. Another process placing an identical order on the same
// account inside the window would still be indistinguishable; that is a
// residual risk of a venue with no client order id, and it is documented on
// the package.
func (b *venueOrderBook) findSubmission(market string, submitted submission, known *oidIndex) (int64, bool, error) {
	since := submitted.at.Add(-recoveryClockSlack).UnixMilli()
	wantSide := sideAsk
	if submitted.wire.IsBuy {
		wantSide = sideBid
	}
	wantPrice, err := decimal.FromDecimalString(submitted.wire.Price)
	if err != nil {
		return 0, false, fmt.Errorf("txflow: submitted price %q is malformed: %w", submitted.wire.Price, err)
	}
	wantSize, err := decimal.FromDecimalString(submitted.wire.Size)
	if err != nil {
		return 0, false, fmt.Errorf("txflow: submitted size %q is malformed: %w", submitted.wire.Size, err)
	}
	var (
		found      int64
		candidates int
	)
	for oid, order := range b.history {
		if *order.Symbol != market || *order.Timestamp < since {
			continue
		}
		if _, tracked := known.lookup(oid); tracked {
			continue
		}
		if *order.Side != wantSide || *order.ReduceOnly != submitted.wire.ReduceOnly {
			continue
		}
		price, err := decimal.FromDecimalString(*order.LimitPx)
		if err != nil {
			return 0, false, fmt.Errorf("txflow: historicalOrders oid %d has malformed limitPx: %w", oid, err)
		}
		size, err := decimal.FromDecimalString(*order.OrigSz)
		if err != nil {
			return 0, false, fmt.Errorf("txflow: historicalOrders oid %d has malformed origSz: %w", oid, err)
		}
		if price.Cmp(wantPrice) != 0 || size.Cmp(wantSize) != 0 {
			continue
		}
		found = oid
		candidates++
	}
	switch candidates {
	case 0:
		return 0, false, nil
	case 1:
		return found, true, nil
	default:
		return 0, false, fmt.Errorf("txflow: %d orders on %s match the ambiguous submission", candidates, market)
	}
}

// settleOrderLocked applies what the venue's records say about a tracked
// order: a live one stays tracked, a filled one is retired silently (its
// fill is reported by the poll), and a closed one is retired with a
// rejection under the caller's reason if a cancel was requested. Callers
// hold stateMu.
func (e *Executor) settleOrderLocked(id godex.OrderID, liveness orderLiveness) {
	switch liveness.state {
	case orderLive:
		return
	case orderFilledOut:
		e.untrackOrderLocked(id)
	case orderClosed:
		// Read before untracking, which clears the cancel intent.
		reason := e.terminalReasonLocked(id, liveness.reason)
		e.untrackOrderLocked(id)
		e.send(godex.OrderRejectedEvent{OrderID: id, Reason: reason})
	}
}

// reconcileOrdersAsync re-checks tracked orders off the socket goroutine.
func (e *Executor) reconcileOrdersAsync() {
	if e.beginOp() != nil {
		return
	}
	go func() {
		defer e.endOp()
		e.reconcileTrackedOrders(e.lifecycleCtx)
	}()
}

// reconcileTrackedOrders checks every order this executor still believes is
// live against the venue's records. orderUpdates is push-only and never
// replayed, so an order cancelled while the socket was down would otherwise
// stay tracked forever and its OrderRejectedEvent would never arrive.
func (e *Executor) reconcileTrackedOrders(ctx context.Context) {
	book, err := e.readOrderBook(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.logger.Error("txflow order reconciliation failed", "error", err)
		}
		return
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if !e.connOpen {
		// A reading that outlived its connection would publish outside a
		// Connected/Disconnected window; the next connection re-reads.
		return
	}
	for id, oid := range e.orders {
		if oid == 0 {
			continue // an ambiguous submission; recovery owns it
		}
		e.settleOrderLocked(id, book.liveness(oid))
	}
}

// assertCrossMargin verifies the account's margin mode for the traded market.
// clearinghouseState omits markets the account is flat in, so a flat
// account's mode is invisible there; activeAssetData reports it either way.
func (e *Executor) assertCrossMargin(ctx context.Context) error {
	response, err := postJSON[activeAssetDataResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeActiveAssetData, User: e.cfg.accountAddress, Coin: e.coinParam()})
	if err != nil {
		return err
	}
	if *response.Leverage.Type != leverageTypeCross {
		return fmt.Errorf("txflow: %s is set to %q margin on this account; only cross is supported",
			e.cfg.market, *response.Leverage.Type)
	}
	return nil
}

// newClientOrderID mints the 128-bit id an order is tracked under. It is
// assigned before submission so an ambiguous outcome still has a handle.
func newClientOrderID() (godex.OrderID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("txflow: generating a client order id failed: %w", err)
	}
	return godex.OrderID("0x" + hex.EncodeToString(raw[:])), nil
}

func (e *Executor) trackOrder(id godex.OrderID, wire orderWire) {
	e.stateMu.Lock()
	e.orders[id] = 0
	e.submissions[id] = submission{wire: wire, at: e.cfg.now()}
	e.stateMu.Unlock()
}

func (e *Executor) bindOrderOid(id godex.OrderID, oid int64) {
	e.stateMu.Lock()
	if _, tracked := e.orders[id]; tracked {
		e.orders[id] = oid
	}
	// Bound even for an order no longer tracked: its fills still need a
	// name.
	e.oids.bind(oid, id)
	filled := false
	if status, orphaned := e.orphanStatuses[oid]; orphaned {
		delete(e.orphanStatuses, oid)
		filled = e.applyOrderStatusLocked(id, status)
	}
	e.stateMu.Unlock()
	if filled {
		e.triggerFillPoll()
	}
}

// applyOrderStatusLocked applies one order update to a tracked order and
// reports whether it filled. Callers hold stateMu.
func (e *Executor) applyOrderStatusLocked(orderID godex.OrderID, status string) bool {
	if _, tracked := e.orders[orderID]; !tracked {
		return false
	}
	switch status {
	case orderStatusOpen, orderStatusPartialFilled, orderStatusTriggered:
		// Still live.
		return false
	case orderStatusFilled:
		// It filled, so a cancel accepted for it applied to nothing and
		// must not be reported as having ended it. The execution comes
		// from the fill poll.
		e.untrackOrderLocked(orderID)
		return true
	default:
		// Every remaining status ends the order without filling it in
		// full; validate() has already rejected anything unrecognized.
		// The reason is read before untracking, which clears the intent.
		reason := e.terminalReasonLocked(orderID, status)
		e.untrackOrderLocked(orderID)
		e.send(godex.OrderRejectedEvent{OrderID: orderID, Reason: reason})
		return false
	}
}

func (e *Executor) untrackOrder(id godex.OrderID) {
	e.stateMu.Lock()
	e.untrackOrderLocked(id)
	e.stateMu.Unlock()
}

func (e *Executor) untrackOrderLocked(id godex.OrderID) {
	delete(e.orders, id)
	delete(e.submissions, id)
	delete(e.canceling, id)
}

// terminalReasonLocked names why an order ended. A cancel the caller asked for
// and the venue accepted reads the same on every venue, so it wins over the
// venue's own wording for the removal it caused — otherwise the reason would
// depend on which of the two reports arrived first.
func (e *Executor) terminalReasonLocked(id godex.OrderID, venueReason string) string {
	if _, requested := e.canceling[id]; requested {
		return godex.ReasonCanceledByRequest
	}
	return venueReason
}

// decodeOrderStatus extracts the single per-order outcome an order action
// returns. The adapter submits exactly one order per action, so any other
// count means the response does not describe the submission.
func decodeOrderStatus(statuses []json.RawMessage) (*orderStatusWire, error) {
	if len(statuses) != 1 {
		return nil, fmt.Errorf("txflow: order response carried %d statuses, want 1", len(statuses))
	}
	var status orderStatusWire
	if err := json.Unmarshal(statuses[0], &status); err != nil {
		return nil, fmt.Errorf("txflow: order status is malformed: %w", err)
	}
	if err := status.validate(); err != nil {
		return nil, err
	}
	return &status, nil
}

// decodeCancelStatus returns the failure message of a cancel outcome, or ""
// when the venue accepted it. Cancels answer with the bare string "success"
// or with an object carrying an error.
func decodeCancelStatus(statuses []json.RawMessage) (string, error) {
	if len(statuses) != 1 {
		return "", fmt.Errorf("txflow: cancel response carried %d statuses, want 1", len(statuses))
	}
	var text string
	if err := json.Unmarshal(statuses[0], &text); err == nil {
		if text != "success" {
			return "", fmt.Errorf("txflow: cancel returned unknown status %q", text)
		}
		return "", nil
	}
	var status orderStatusWire
	if err := json.Unmarshal(statuses[0], &status); err != nil {
		return "", fmt.Errorf("txflow: cancel status is malformed: %w", err)
	}
	if err := status.validate(); err != nil {
		return "", err
	}
	switch {
	case status.Error != nil:
		return *status.Error, nil
	case status.Success != nil:
		return "", nil
	default:
		return "", fmt.Errorf("txflow: cancel status carried no recognized outcome")
	}
}

// --- account stream ---

func (e *Executor) handleSocketOpen() error {
	e.stateMu.Lock()
	e.connGeneration++
	e.connOpen = true
	reconnected := e.connGeneration > 1
	e.stateMu.Unlock()

	// Connected is emitted before subscribing, not after: the socket's read
	// loop is already running by the time this hook is called, so an update
	// answering the subscription could otherwise be handled first. Announcing
	// the connection first is what keeps it inside the Connected/Disconnected
	// window the contract promises.
	e.emitEvent(godex.ConnectedEvent{VenueID: godex.VenueTxFlow})

	if err := e.sendSubscribe(channelOrderUpdates); err != nil {
		return err
	}

	if reconnected {
		// Position, margin and fills are read rather than pushed, so a
		// reconnect re-converges from fresh reads instead of waiting for the
		// next poll ticks. Orders are reconciled for a different reason: their
		// updates are push-only, so a cancellation that happened while the
		// socket was down is never replayed.
		e.refreshAccountAsync()
		e.reconcileOrdersAsync()
		e.triggerFillPoll()
	}
	return nil
}

// sendSubscribe sends the venue's subscription envelope, which names the
// channel twice: once beside the method and once inside the subscription.
func (e *Executor) sendSubscribe(channel string) error {
	message, err := json.Marshal(map[string]any{
		"method": "subscribe",
		"type":   channel,
		"subscription": map[string]string{
			"type": channel,
			"user": e.cfg.accountAddress,
		},
	})
	if err != nil {
		return err
	}
	return e.socket.Send(string(message))
}

func (e *Executor) handleSocketDown() {
	e.stateMu.Lock()
	e.connOpen = false
	e.stateMu.Unlock()
	e.emitEvent(godex.DisconnectedEvent{VenueID: godex.VenueTxFlow})
}

func (e *Executor) handleSocketMessage(raw []byte) error {
	var envelope wsEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("txflow: ws message is malformed JSON: %w", err)
	}
	if err := envelope.validate(); err != nil {
		return err
	}
	if envelope.Channel == nil {
		if *envelope.Method == wsMethodPong {
			return nil
		}
		return fmt.Errorf("txflow: ws message with unexpected method %q", *envelope.Method)
	}
	switch *envelope.Channel {
	case channelSubscriptionResponse:
		return nil
	case channelError:
		return fmt.Errorf("txflow: ws error notice: %s", truncate(envelope.Data))
	case channelOrderUpdates:
		return e.handleOrderUpdates(envelope.Data)
	default:
		return fmt.Errorf("txflow: ws message on unexpected channel %q", *envelope.Channel)
	}
}

func (e *Executor) handleOrderUpdates(data []byte) error {
	var updates []wsOrderUpdate
	if err := json.Unmarshal(data, &updates); err != nil {
		return fmt.Errorf("txflow: orderUpdates payload is malformed: %w", err)
	}
	for i := range updates {
		if err := updates[i].validate(); err != nil {
			return err
		}
	}

	filled := false
	e.stateMu.Lock()
	for i := range updates {
		update := &updates[i]
		oid := *update.Order.Oid
		orderID, known := e.oids.lookup(oid)
		if !known {
			e.rememberOrphanLocked(oid, *update.Status)
			continue
		}
		if e.applyOrderStatusLocked(orderID, *update.Status) {
			filled = true
		}
	}
	e.stateMu.Unlock()
	if filled {
		e.triggerFillPoll()
	}
	return nil
}

// orphanStatusCapacity bounds the remembered updates for unbound oids. Almost
// all of them belong to orders this executor did not place (another process
// on the account) and are never claimed; the map is reset rather than grown.
const orphanStatusCapacity = 1024

// rememberOrphanLocked keeps the latest status seen for an oid no order is
// bound to yet. Callers hold stateMu.
func (e *Executor) rememberOrphanLocked(oid int64, status string) {
	if len(e.orphanStatuses) >= orphanStatusCapacity {
		e.orphanStatuses = make(map[int64]string, orphanStatusCapacity)
	}
	e.orphanStatuses[oid] = status
}

// --- fills ---

// seedFillHistory absorbs the account's existing executions so the first
// poll does not publish them as this executor's work.
func (e *Executor) seedFillHistory(ctx context.Context) error {
	fills, err := e.readFills(ctx)
	if err != nil {
		return err
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.fillHistorySeeded {
		return nil
	}
	for i := range *fills {
		e.fills.Observe(*(*fills)[i].Tid)
	}
	e.fillHistorySeeded = true
	return nil
}

func (e *Executor) readFills(ctx context.Context) (*fillList, error) {
	return postJSON[fillList](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeUserFills, User: e.cfg.accountAddress})
}

// pollFills reads the account's recent executions and publishes the ones not
// yet seen. It reports whether anything was published.
func (e *Executor) pollFills(ctx context.Context) (bool, error) {
	fills, err := e.readFills(ctx)
	if err != nil {
		return false, err
	}
	nctx := e.normalizeContext()

	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if !e.connOpen {
		// Fills are account events and belong inside a Connected window. A
		// read that landed while the socket is down is left for the next
		// poll, which will see the same executions again.
		return false, nil
	}
	// While a submission's oid is still unknown — its response in flight, or
	// lost and awaiting recovery — a fill under an oid this executor does not
	// recognize may be that order's. Publishing it now would name no order
	// and spend its trade id, so it waits for a poll after the oid is bound.
	bindPending := false
	for _, oid := range e.orders {
		if oid == 0 {
			bindPending = true
			break
		}
	}
	published := false
	for i := range *fills {
		fill := &(*fills)[i]
		// Normalization runs before the trade id is remembered. A fill that
		// fails to normalize aborts nothing but is left unseen, so the next
		// poll — and the log — get another look at it.
		orderID, known := e.oids.lookup(*fill.Oid)
		if !known && bindPending {
			continue
		}
		event, err := normalizeFill(fill, orderID, nctx)
		if err != nil {
			return published, err
		}
		if event == nil {
			continue // a market this executor does not manage
		}
		if !e.fills.Observe(*fill.Tid) {
			continue
		}
		e.send(*event)
		published = true
	}
	return published, nil
}

// triggerFillPoll wakes the fill poller ahead of its next tick.
func (e *Executor) triggerFillPoll() {
	select {
	case e.fillPollTrigger <- struct{}{}:
	default:
	}
}

// fillPollLoop is the executor's only source of executions: the venue has no
// fills stream. Every tick reads the account's recent fills; the dedupe cache
// makes each execution publish exactly once.
func (e *Executor) fillPollLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(e.cfg.fillPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.lifecycleCtx.Done():
			return
		case <-ticker.C:
		case <-e.fillPollTrigger:
		}
		if !e.socket.IsOpen() {
			continue
		}
		published, err := e.pollFills(e.lifecycleCtx)
		if err != nil && e.lifecycleCtx.Err() == nil {
			e.logger.Error("txflow fill poll failed", "error", err)
		}
		if published {
			// A fill moved the position; read the new one rather than
			// waiting for the next account poll tick.
			e.refreshAccountAsync()
		}
	}
}

// --- account state ---

// refreshAccountAsync reads the account off the caller's goroutine. Failures
// are logged; the poll loop retries.
func (e *Executor) refreshAccountAsync() {
	if e.beginOp() != nil {
		return
	}
	go func() {
		defer e.endOp()
		if _, err := e.refreshAccount(e.lifecycleCtx); err != nil && e.lifecycleCtx.Err() == nil {
			e.logger.Error("txflow account refresh failed", "error", err)
		}
	}()
}

// readAccount fetches the clearinghouse snapshot without interpreting it.
func (e *Executor) readAccount(ctx context.Context) (*clearinghouseState, error) {
	return postJSON[clearinghouseState](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeClearinghouseState, User: e.cfg.accountAddress})
}

// refreshAccount reads the clearinghouse snapshot and emits position and
// margin. The observation sequence is reserved before the fetch starts so a
// slow response cannot overwrite newer state. Reports whether fresh state was
// applied.
func (e *Executor) refreshAccount(ctx context.Context) (bool, error) {
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	generation := e.connGeneration
	e.stateMu.Unlock()

	state, err := e.readAccount(ctx)
	if err != nil {
		return false, err
	}
	snapshot, err := normalizeAccount(state, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return false, err
	}
	if snapshot.needsRefresh {
		// A position with size but no price is a moment in flight, not a
		// state to publish. Report "not applied" so the caller re-reads.
		return false, nil
	}
	applied := e.applyStateEvents([]godex.AccountEvent{
		godex.PositionEvent{Position: snapshot.position},
		snapshot.margin,
	}, sequence, generation)
	e.clearAccountInvalid(sequence, applied)
	return applied, nil
}

// applyStateEvents emits a snapshot batch, dropping stale position and margin
// observations. Reports whether fresh state was applied.
func (e *Executor) applyStateEvents(events []godex.AccountEvent, sequence int64, generation int) bool {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	// A read that outlived its connection describes an account the consumer
	// is no longer being told about; publishing it would put state events
	// outside a Connected/Disconnected window, or attribute one connection's
	// state to the next.
	if !e.connOpen || generation != e.connGeneration {
		return false
	}
	stale := sequence < e.lastStateObservation ||
		(e.accountInvalid != nil && sequence <= e.accountInvalid.sequence)
	if stale {
		return false
	}
	e.lastStateObservation = sequence
	for _, event := range events {
		switch event.(type) {
		case godex.PositionEvent:
			e.hasPositionSnapshot = true
		case godex.MarginEvent:
			e.hasMarginSnapshot = true
		}
		e.send(event)
	}
	return true
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
		symbol:     e.cfg.symbol,
		market:     e.cfg.market,
		receivedAt: e.cfg.now(),
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
				_ = e.socket.Send(wsMethodPing)
			}
		}
	}
}

// accountPollLoop backstops the fill-triggered refresh so position and margin
// still converge after changes this executor did not cause — funding,
// liquidation, or another process trading the same account.
func (e *Executor) accountPollLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(e.cfg.accountPollInterval)
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
				e.logger.Error("txflow account poll failed", "error", err)
			}
		}
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
// outcome — the venue's answer to the submission and the account stream's
// order update — and neither is ordered against the other, so the losing path
// is dropped here rather than at each call site. The dropped copy carries no
// news: OrderRejectedEvent means the order is finished, which is not a fact
// that can arrive twice with different content.
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

// emitEvent delivers a single event outside a snapshot batch.
func (e *Executor) emitEvent(event godex.AccountEvent) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.send(event)
}
