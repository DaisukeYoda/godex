// Package hyperliquid implements godex.VenueExecutor for Hyperliquid. It
// signs limit orders (post-only / IOC) with an API (agent) wallet and submits
// them to the /exchange endpoint, observing the account through the
// authenticated userFills and orderUpdates streams plus clearinghouse
// snapshots.
package hyperliquid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/internal/dedupe"
	"github.com/DaisukeYoda/godex/internal/ws"
)

const wsLabel = "hyperliquid-account"

// assetMeta is the resolved venue metadata for the traded perp.
type assetMeta struct {
	// index is the perp's position in the universe array, which is the id
	// orders and cancels are keyed by.
	index      int
	szDecimals int
	// maintenanceLeverage is the strictest tier of the perp's margin
	// schedule, not the headline max leverage.
	maintenanceLeverage int
}

type accountInvalidObservation struct {
	err      error
	sequence int64
}

// Executor is the Hyperliquid implementation of godex.VenueExecutor.
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
	orders               map[godex.OrderID]int64 // client order id -> venue oid (0 = not yet known)
	ordersByOid          map[int64]godex.OrderID
	// canceling holds orders whose cancel the venue accepted. They stay
	// tracked, because only the account stream (or reconciliation) can say
	// how the order actually ended — a cancel accepted the instant the order
	// filled applies to nothing. Membership is what makes a second cancel
	// unaddressable and what labels the terminal event when it arrives.
	canceling map[godex.OrderID]struct{}
	fills     *fillCache
	// connGeneration counts connections; connOpen tracks whether the current
	// one is up. Account reads run off the socket, so a slow response can
	// land after its connection dropped — the pair is what keeps those
	// results from being published outside a Connected/Disconnected window.
	connGeneration int
	connOpen       bool
	// fillHistorySeeded records that the first userFills snapshot has been
	// absorbed. That snapshot is the account's history, not this executor's
	// work, so it seeds the dedupe cache without being published; later
	// snapshots (one per reconnect) publish whatever they carry that the
	// cache has not seen, which is exactly the fills missed while down.
	fillHistorySeeded bool
	// fillSnapshotReady closes once that first snapshot has been absorbed.
	// Connect waits on it: accepting an order earlier would let its fill
	// arrive inside a late snapshot and be discarded as history.
	fillSnapshotReady chan struct{}
	fillSnapshotOnce  sync.Once

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
		cfg:               resolved,
		logger:            resolved.logger,
		events:            make(chan godex.AccountEvent, godex.DefaultAccountEventBuffer),
		rejections:        dedupe.NewSet[godex.OrderID](dedupe.RejectionCapacity),
		lifecycleCtx:      lifecycleCtx,
		lifecycleCancel:   lifecycleCancel,
		orders:            make(map[godex.OrderID]int64),
		ordersByOid:       make(map[int64]godex.OrderID),
		canceling:         make(map[godex.OrderID]struct{}),
		fills:             newFillCache(),
		fillSnapshotReady: make(chan struct{}),
	}, nil
}

// VenueID implements godex.VenueExecutor.
func (e *Executor) VenueID() godex.VenueID {
	return godex.VenueHyperliquid
}

// AccountEvents implements godex.VenueExecutor.
func (e *Executor) AccountEvents() <-chan godex.AccountEvent {
	return e.events
}

// Connect implements godex.VenueExecutor: it resolves the perp's asset id and
// quantization, builds the signer, starts the account streams, and completes
// only after a verified clearinghouse snapshot has been emitted.
func (e *Executor) Connect(ctx context.Context) (godex.ExecutionMetadata, error) {
	e.opMu.Lock()
	if e.closed {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, godex.ErrClosed
	}
	if e.connected {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, fmt.Errorf("hyperliquid: executor already connected")
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
	e.logger.Info("hyperliquid signer ready",
		"agent_address", sgnr.address(), "account", e.cfg.userAddress, "coin", e.cfg.coin)
	e.warnIfAgentUnlisted(ctx, sgnr.address())

	// Margin mode is checked before the socket opens: an order action does
	// not carry one, so an account left in isolated mode would silently open
	// a position the adapter's whole-account liquidation math cannot describe.
	if err := e.assertCrossMargin(ctx); err != nil {
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

	if err := e.awaitFillSnapshot(ctx); err != nil {
		_ = e.socket.Stop()
		return godex.ExecutionMetadata{}, err
	}
	if err := e.applyInitialSnapshot(ctx); err != nil {
		_ = e.socket.Stop()
		return godex.ExecutionMetadata{}, err
	}

	e.pollerWG.Add(2)
	go e.pingLoop()
	go e.accountPollLoop()

	e.txMu.Lock()
	e.acceptingTx = true
	e.txMu.Unlock()

	marginFraction, err := maintenanceMarginFraction(asset.maintenanceLeverage)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	return godex.ExecutionMetadata{
		SizeStep:                  decimal.New(1, asset.szDecimals),
		MaintenanceMarginFraction: marginFraction,
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

// loadAssetMeta resolves the configured coin to its asset id and
// quantization. The id is the coin's index in the universe array, so the
// lookup must walk the array rather than trust any field inside an entry.
func (e *Executor) loadAssetMeta(ctx context.Context) (assetMeta, error) {
	response, err := postJSON[metaResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeMeta})
	if err != nil {
		return assetMeta{}, err
	}
	tables := response.marginTablesByID()
	for index := range *response.Universe {
		entry := &(*response.Universe)[index]
		if *entry.Name != e.cfg.coin {
			continue
		}
		if entry.IsDelisted != nil && *entry.IsDelisted {
			return assetMeta{}, fmt.Errorf("hyperliquid: perp %s is delisted", e.cfg.coin)
		}
		if entry.OnlyIsolated != nil && *entry.OnlyIsolated {
			return assetMeta{}, fmt.Errorf("hyperliquid: perp %s is isolated-margin only, which is unsupported", e.cfg.coin)
		}
		leverage, err := maintenanceLeverage(entry, tables)
		if err != nil {
			return assetMeta{}, err
		}
		return assetMeta{index: index, szDecimals: *entry.SzDecimals, maintenanceLeverage: leverage}, nil
	}
	return assetMeta{}, fmt.Errorf("hyperliquid: perp not found in universe: %s", e.cfg.coin)
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
		return fmt.Errorf("hyperliquid: initial account snapshot was not fully applied")
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
		return godex.OrderAck{}, fmt.Errorf("hyperliquid: account state is invalid: %w", invalid.err)
	}
	if order.Symbol != e.cfg.symbol {
		return godex.OrderAck{}, fmt.Errorf("hyperliquid: executor is configured for %s, got %s", e.cfg.symbol, order.Symbol)
	}
	if e.signer == nil {
		return godex.OrderAck{}, godex.ErrNotConnected
	}

	tick, err := priceTick(order.Price, e.asset.szDecimals)
	if err != nil {
		return godex.OrderAck{}, err
	}
	price, err := godex.RoundPriceToTick(order.Price, tick, order.Side)
	if err != nil {
		return godex.OrderAck{}, err
	}
	step := decimal.New(1, e.asset.szDecimals)
	var size decimal.Decimal
	if order.ReduceOnly {
		size, err = godex.QuantizeReduceOnlySize(order.Size, step)
	} else {
		size, err = godex.QuantizeSize(order.Size, step, step)
	}
	if err != nil {
		return godex.OrderAck{}, err
	}

	tif := tifIOC
	if order.Intent == godex.IntentPostOnly {
		tif = tifALO
	} else if order.Intent != godex.IntentIOC {
		return godex.OrderAck{}, fmt.Errorf("hyperliquid: unsupported order intent %q", order.Intent)
	}

	orderID, err := newClientOrderID()
	if err != nil {
		return godex.OrderAck{}, err
	}
	action := orderAction{
		Type: actionTypeOrder,
		Orders: []orderWire{{
			Asset:      e.asset.index,
			IsBuy:      order.Side == godex.SideBuy,
			Price:      wireDecimal(price),
			Size:       wireDecimal(size),
			ReduceOnly: order.ReduceOnly,
			OrderType:  orderTypeWire{Limit: limitOrderWire{Tif: tif}},
			Cloid:      string(orderID),
		}},
		Grouping: groupingNA,
	}

	// Track before submitting: if the outcome turns out to be unknown, the
	// order id must already be known so reconciliation can ask the venue
	// about it.
	e.trackOrder(orderID)
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
		return godex.OrderAck{}, fmt.Errorf("hyperliquid: order placement failed: %s", failure)
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
				OrderID: orderID, VenueID: godex.VenueHyperliquid,
				Status: godex.AckRejected, Time: e.cfg.now(),
			}, nil
		}
		return godex.OrderAck{}, fmt.Errorf("hyperliquid: order rejected: %s", *status.Error)
	case status.Resting != nil:
		e.bindOrderOid(orderID, *status.Resting.Oid)
	case status.Filled != nil:
		// The execution itself is reported by the account stream, which is
		// the only source of truth for fills; the oid is bound so a
		// subsequent order update can be attributed.
		e.bindOrderOid(orderID, *status.Filled.Oid)
	default:
		return godex.OrderAck{}, e.latchTxFault(
			fmt.Errorf("hyperliquid: order status carried no recognized outcome"), orderID)
	}
	return godex.OrderAck{
		OrderID: orderID, VenueID: godex.VenueHyperliquid,
		Status: godex.AckSubmitted, Time: e.cfg.now(),
	}, nil
}

// CancelOrder implements godex.VenueExecutor. Cancellation is keyed by the
// client order id assigned before submission, so it stays possible even when
// the placing response was lost.
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
	_, tracked := e.orders[id]
	_, alreadyCanceling := e.canceling[id]
	if tracked && !alreadyCanceling {
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

	action := cancelByCloidAction{
		Type:    actionTypeCancelByCloid,
		Cancels: []cancelByCloidWire{{Asset: e.asset.index, Cloid: string(id)}},
	}
	// A cancel that did not take leaves the order addressable again, so the
	// intent is withdrawn on every path that does not return success —
	// including an unknown outcome, because cancel-by-cloid is idempotent and
	// retrying it is how that fault recovers. The cost is that a cancel which
	// did apply after an unknown outcome is reported under the venue's own
	// wording; that is the honest answer, since the adapter never learned its
	// cancel was the cause.
	statuses, failure, err := e.submitAction(ctx, action, id)
	if err != nil {
		e.clearCancelIntent(id)
		return err
	}
	if failure != "" {
		e.clearCancelIntent(id)
		return fmt.Errorf("hyperliquid: cancel failed: %s", failure)
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
		return fmt.Errorf("hyperliquid: cancel failed: %s", message)
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
// order that turns out to have filled is retired without a rejection; one the
// venue cannot classify stays tracked for the next reconciliation.
func (e *Executor) resolveGoneOrder(ctx context.Context, id godex.OrderID) {
	liveness, reason, err := e.queryOrderLiveness(ctx, id)
	if err != nil {
		e.logger.Warn("hyperliquid could not resolve a cancelled order's outcome; "+
			"it stays tracked for reconciliation", "order_id", id, "error", err)
		return
	}
	switch liveness {
	case orderLive:
		// The venue holds it after all; the cancel did not apply.
		return
	case orderLivenessUnclear:
		return
	case orderFilledOut:
		e.untrackOrder(id)
		return
	}
	// The venue had nothing to cancel, so the caller's cancel is not how this
	// order ended and must not be what it is reported under. Whatever the
	// venue says retired it is the truthful answer; untracking clears the
	// intent along with the order.
	e.stateMu.Lock()
	_, still := e.orders[id]
	e.untrackOrderLocked(id)
	e.stateMu.Unlock()
	if still {
		e.emitEvent(godex.OrderRejectedEvent{OrderID: id, Reason: reason})
	}
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
		return nil, "", fmt.Errorf("hyperliquid: executor is not accepting submissions: %w", godex.ErrNotConnected)
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
	if len(e.cfg.vaultAddress) != 0 {
		request.VaultAddress = normalizeAddress(e.cfg.vaultAddress)
	}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	statuses, failure, err := postExchange(requestCtx, e.cfg.httpClient, e.cfg.restBaseURL, request)
	if err != nil {
		if e.lifecycleCtx.Err() != nil {
			return nil, "", fmt.Errorf("hyperliquid: submission lifecycle ended: %w", e.lifecycleCtx.Err())
		}
		return nil, "", e.latchTxFaultLocked(err, orderID)
	}
	return statuses, failure, nil
}

func (e *Executor) assertTxCanStartLocked(ctx context.Context) error {
	if err := e.lifecycleCtx.Err(); err != nil {
		return fmt.Errorf("hyperliquid: submission lifecycle ended: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("hyperliquid: submission canceled before dispatch: %w", err)
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

// recoverTxFault asks the venue whether the ambiguous submission's order
// exists. That answer — and not a retry — is what resolves the ambiguity: an
// order the venue never took is untracked, one it holds stays tracked and
// cancellable. Unreachable endpoints reschedule with backoff.
func (e *Executor) recoverTxFault() {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	e.faultTimer = nil
	if e.txFault == nil || !e.acceptingTx || e.lifecycleCtx.Err() != nil {
		return
	}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	orderID := e.txFaultOrderID
	if orderID != "" {
		known, err := e.orderExists(requestCtx, orderID)
		if err != nil {
			if e.acceptingTx && e.lifecycleCtx.Err() == nil {
				e.scheduleFaultRecoveryLocked()
			}
			return
		}
		if known {
			// The caller never received an ack, so it does not know this
			// order's id and cannot cancel it. Leaving it resting would be an
			// exposure nobody can address; cancelling makes the venue agree
			// with what the caller already believes. If it filled first, the
			// account stream still reports that — cancelling is idempotent.
			if err := e.cancelForRecoveryLocked(requestCtx, orderID); err != nil {
				e.logger.Error("hyperliquid could not cancel the order left by an unknown outcome",
					"order_id", orderID, "error", err)
				if e.acceptingTx && e.lifecycleCtx.Err() == nil {
					e.scheduleFaultRecoveryLocked()
				}
				return
			}
		}
		e.untrackOrder(orderID)
		e.logger.Info("hyperliquid submission reconciled",
			"order_id", orderID, "venue_held_order", known)
	} else if _, err := e.readAccount(requestCtx); err != nil {
		if e.acceptingTx && e.lifecycleCtx.Err() == nil {
			e.scheduleFaultRecoveryLocked()
		}
		return
	}
	e.txFault = nil
	e.txFaultOrderID = ""
}

// orderExists reports whether the venue holds a record of the client order
// id. "unknownOid" is a definitive no: the submission never landed.
func (e *Executor) orderExists(ctx context.Context, orderID godex.OrderID) (bool, error) {
	response, err := postJSON[orderQueryResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeOrderStatus, User: e.cfg.userAddress, Oid: string(orderID)})
	if err != nil {
		return false, err
	}
	return *response.Status == queryStatusOrder, nil
}

// cancelForRecoveryLocked cancels an order the venue turned out to be holding
// after an unknown outcome. It bypasses the fault latch deliberately: the
// latch exists to stop *new* exposure, and this is the call that removes the
// exposure already there. Callers hold txMu.
func (e *Executor) cancelForRecoveryLocked(ctx context.Context, orderID godex.OrderID) error {
	action := cancelByCloidAction{
		Type:    actionTypeCancelByCloid,
		Cancels: []cancelByCloidWire{{Asset: e.asset.index, Cloid: string(orderID)}},
	}
	nonce := e.nextNonceLocked()
	sig, err := e.signer.signAction(action, nonce)
	if err != nil {
		return err
	}
	request := exchangeRequest{Action: action, Nonce: nonce, Signature: sig}
	if len(e.cfg.vaultAddress) != 0 {
		request.VaultAddress = normalizeAddress(e.cfg.vaultAddress)
	}
	statuses, failure, err := postExchange(ctx, e.cfg.httpClient, e.cfg.restBaseURL, request)
	if err != nil {
		return err
	}
	if failure != "" {
		return fmt.Errorf("hyperliquid: recovery cancel refused: %s", failure)
	}
	message, err := decodeCancelStatus(statuses)
	if err != nil {
		return err
	}
	if message != "" && !cancelAlreadyGonePattern.MatchString(message) {
		return fmt.Errorf("hyperliquid: recovery cancel failed: %s", message)
	}
	return nil
}

// orderLiveness is what the venue says about an order the executor tracks.
type orderLiveness int

const (
	// orderLive means the venue still holds the order on the book.
	orderLive orderLiveness = iota
	// orderFilledOut means it finished by filling, which closes it without
	// being a rejection.
	orderFilledOut
	// orderClosed means it ended without filling in full.
	orderClosed
	// orderLivenessUnclear means the venue holds the order but did not report
	// its lifecycle status, so the executor keeps tracking it rather than
	// inventing an answer.
	orderLivenessUnclear
)

// queryOrderLiveness asks the venue about one tracked order.
func (e *Executor) queryOrderLiveness(ctx context.Context, orderID godex.OrderID) (orderLiveness, string, error) {
	response, err := postJSON[orderQueryResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeOrderStatus, User: e.cfg.userAddress, Oid: string(orderID)})
	if err != nil {
		return orderLivenessUnclear, "", err
	}
	if *response.Status != queryStatusOrder {
		return orderClosed, "the venue does not hold this order", nil
	}
	if response.Order == nil || response.Order.Status == nil {
		return orderLivenessUnclear, "", nil
	}
	switch status := *response.Order.Status; status {
	case orderStatusOpen, orderStatusTriggered:
		return orderLive, "", nil
	case orderStatusFilled:
		return orderFilledOut, "", nil
	default:
		return orderClosed, status, nil
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

// reconcileTrackedOrders asks the venue about every order this executor still
// believes is live. orderUpdates is push-only and never replayed, so an order
// cancelled while the socket was down would otherwise stay tracked forever and
// its OrderRejectedEvent would never arrive.
func (e *Executor) reconcileTrackedOrders(ctx context.Context) {
	e.stateMu.Lock()
	tracked := make([]godex.OrderID, 0, len(e.orders))
	for id := range e.orders {
		tracked = append(tracked, id)
	}
	e.stateMu.Unlock()

	for _, orderID := range tracked {
		liveness, reason, err := e.queryOrderLiveness(ctx, orderID)
		if err != nil {
			if ctx.Err() == nil {
				e.logger.Error("hyperliquid order reconciliation failed",
					"order_id", orderID, "error", err)
			}
			return
		}
		if liveness == orderLive || liveness == orderLivenessUnclear {
			continue
		}
		e.stateMu.Lock()
		if _, still := e.orders[orderID]; still {
			// Read before untracking, which clears the cancel intent.
			terminalReason := e.terminalReasonLocked(orderID, reason)
			e.untrackOrderLocked(orderID)
			if liveness == orderClosed {
				e.send(godex.OrderRejectedEvent{OrderID: orderID, Reason: terminalReason})
			}
		}
		e.stateMu.Unlock()
	}
}

// awaitFillSnapshot blocks until the venue's opening userFills snapshot has
// been absorbed. Until then the executor cannot tell an account's history from
// its own executions, so accepting an order first risks suppressing its fill.
func (e *Executor) awaitFillSnapshot(ctx context.Context) error {
	timeout := time.NewTimer(e.cfg.fillSnapshotTimeout)
	defer timeout.Stop()
	select {
	case <-e.fillSnapshotReady:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("hyperliquid: canceled awaiting the initial fill snapshot: %w", ctx.Err())
	case <-e.lifecycleCtx.Done():
		return godex.ErrClosed
	case <-timeout.C:
		return fmt.Errorf("hyperliquid: no userFills snapshot arrived within %s", e.cfg.fillSnapshotTimeout)
	}
}

// assertCrossMargin verifies the account's margin mode for the traded coin.
// clearinghouseState omits coins the account is flat in, so a flat account's
// mode is invisible there; activeAssetData reports it either way. An order
// action carries no margin mode, so an account left in isolated mode would
// open a position this adapter's whole-account liquidation math cannot
// describe.
func (e *Executor) assertCrossMargin(ctx context.Context) error {
	response, err := postJSON[activeAssetDataResponse](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeActiveAssetData, User: e.cfg.userAddress, Coin: e.cfg.coin})
	if err != nil {
		return err
	}
	if *response.Leverage.Type != leverageTypeCross {
		return fmt.Errorf("hyperliquid: %s is set to %q margin on this account; only cross is supported",
			e.cfg.coin, *response.Leverage.Type)
	}
	return nil
}

// warnIfAgentUnlisted reports a signing key that is not a listed agent of the
// account. It warns rather than fails: the venue also has a single unnamed
// agent slot that this listing does not cover, so refusing here would reject
// working configurations. A genuinely wrong key still fails at the first
// order — this only makes that outcome predictable at connect time.
func (e *Executor) warnIfAgentUnlisted(ctx context.Context, agentAddress string) {
	if agentAddress == e.cfg.accountAddress {
		return // the account signs for itself
	}
	agents, err := postJSON[extraAgentList](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeExtraAgents, User: e.cfg.accountAddress})
	if err != nil {
		e.logger.Warn("hyperliquid could not list the account's agents", "error", err)
		return
	}
	for _, agent := range *agents {
		if strings.EqualFold(*agent.Address, agentAddress) {
			return
		}
	}
	e.logger.Warn("hyperliquid signing key is not a listed agent for the account; "+
		"this is expected for the venue's unnamed agent slot, but a mismatched key will "+
		"only be refused at the first order",
		"agent_address", agentAddress, "account", e.cfg.accountAddress)
}

// newClientOrderID mints the 128-bit client order id an order is submitted
// under. It is assigned before submission so an ambiguous outcome still has a
// handle to reconcile and cancel by.
func newClientOrderID() (godex.OrderID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("hyperliquid: generating a client order id failed: %w", err)
	}
	return godex.OrderID("0x" + hex.EncodeToString(raw[:])), nil
}

func (e *Executor) trackOrder(id godex.OrderID) {
	e.stateMu.Lock()
	e.orders[id] = 0
	e.stateMu.Unlock()
}

func (e *Executor) bindOrderOid(id godex.OrderID, oid int64) {
	e.stateMu.Lock()
	if _, tracked := e.orders[id]; tracked {
		e.orders[id] = oid
		e.ordersByOid[oid] = id
	}
	e.stateMu.Unlock()
}

func (e *Executor) untrackOrder(id godex.OrderID) {
	e.stateMu.Lock()
	e.untrackOrderLocked(id)
	e.stateMu.Unlock()
}

func (e *Executor) untrackOrderLocked(id godex.OrderID) {
	if oid, tracked := e.orders[id]; tracked {
		if oid != 0 {
			delete(e.ordersByOid, oid)
		}
		delete(e.orders, id)
	}
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
		return nil, fmt.Errorf("hyperliquid: order response carried %d statuses, want 1", len(statuses))
	}
	var status orderStatusWire
	if err := json.Unmarshal(statuses[0], &status); err != nil {
		return nil, fmt.Errorf("hyperliquid: order status is malformed: %w", err)
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
		return "", fmt.Errorf("hyperliquid: cancel response carried %d statuses, want 1", len(statuses))
	}
	var text string
	if err := json.Unmarshal(statuses[0], &text); err == nil {
		if text != "success" {
			return "", fmt.Errorf("hyperliquid: cancel returned unknown status %q", text)
		}
		return "", nil
	}
	var status orderStatusWire
	if err := json.Unmarshal(statuses[0], &status); err != nil {
		return "", fmt.Errorf("hyperliquid: cancel status is malformed: %w", err)
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
		return "", fmt.Errorf("hyperliquid: cancel status carried no recognized outcome")
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
	// loop is already running by the time this hook is called, so a snapshot
	// answering the first subscription can be handled while the second is
	// still being sent. Announcing the connection first is what keeps those
	// fills inside the Connected/Disconnected window the contract promises.
	e.emitEvent(godex.ConnectedEvent{VenueID: godex.VenueHyperliquid})

	for _, subscription := range []string{channelUserFills, channelOrderUpdates} {
		if err := e.sendSubscribe(subscription); err != nil {
			return err
		}
	}

	if reconnected {
		// Position and margin are read rather than pushed, so a reconnect
		// re-converges from a fresh snapshot instead of waiting for the next
		// poll tick. Orders are reconciled for a different reason: their
		// updates are push-only, so a cancellation that happened while the
		// socket was down is never replayed.
		e.refreshAccountAsync()
		e.reconcileOrdersAsync()
	}
	return nil
}

func (e *Executor) sendSubscribe(channel string) error {
	message, err := json.Marshal(map[string]any{
		"method": "subscribe",
		"subscription": map[string]string{
			"type": channel,
			"user": e.cfg.userAddress,
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
	e.emitEvent(godex.DisconnectedEvent{VenueID: godex.VenueHyperliquid})
}

func (e *Executor) handleSocketMessage(raw []byte) error {
	var envelope wsEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("hyperliquid: ws message is malformed JSON: %w", err)
	}
	if err := envelope.validate(); err != nil {
		return err
	}
	switch *envelope.Channel {
	case channelPong, channelSubscriptionResponse:
		return nil
	case channelError:
		return fmt.Errorf("hyperliquid: ws error notice: %s", truncate(envelope.Data))
	case channelUserFills:
		return e.handleUserFills(envelope.Data)
	case channelOrderUpdates:
		return e.handleOrderUpdates(envelope.Data)
	default:
		return fmt.Errorf("hyperliquid: ws message on unexpected channel %q", *envelope.Channel)
	}
}

func (e *Executor) handleUserFills(data []byte) error {
	var payload wsUserFills
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("hyperliquid: userFills payload is malformed: %w", err)
	}
	if err := payload.validate(); err != nil {
		return err
	}

	ctx := e.normalizeContext()
	events := make([]godex.AccountEvent, 0, len(*payload.Fills))

	e.stateMu.Lock()
	// Only a snapshot can be history, and only the first one is history this
	// executor did not do. Live updates are never suppressed, so a venue that
	// sends no snapshot at all still delivers every fill.
	isSnapshot := payload.IsSnapshot != nil && *payload.IsSnapshot
	seeding := isSnapshot && !e.fillHistorySeeded
	if isSnapshot {
		e.fillHistorySeeded = true
	}
	for i := range *payload.Fills {
		fill := &(*payload.Fills)[i]
		// Normalization runs before the trade id is remembered. A fill that
		// fails to normalize aborts the connection, and the snapshot that
		// follows the reconnect is the only chance to see it again — marking
		// it seen first would drop it silently on that second pass.
		event, err := normalizeFill(fill, ctx)
		if err != nil {
			e.stateMu.Unlock()
			return err
		}
		if event == nil {
			continue // a coin this executor does not manage
		}
		if !e.fills.observe(*fill.Tid) || seeding {
			continue
		}
		events = append(events, *event)
	}
	for _, event := range events {
		e.send(event)
	}
	e.stateMu.Unlock()

	if isSnapshot {
		e.fillSnapshotOnce.Do(func() { close(e.fillSnapshotReady) })
	}

	if len(events) > 0 {
		// A fill moved the position; read the new one rather than waiting
		// for the next poll tick.
		e.refreshAccountAsync()
	}
	return nil
}

func (e *Executor) handleOrderUpdates(data []byte) error {
	var updates []wsOrderUpdate
	if err := json.Unmarshal(data, &updates); err != nil {
		return fmt.Errorf("hyperliquid: orderUpdates payload is malformed: %w", err)
	}
	for i := range updates {
		if err := updates[i].validate(); err != nil {
			return err
		}
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	for i := range updates {
		update := &updates[i]
		if *update.Order.Coin != e.cfg.coin {
			continue
		}
		orderID, tracked := e.resolveOrderIDLocked(update)
		if !tracked {
			continue
		}
		switch *update.Status {
		case orderStatusOpen, orderStatusTriggered:
			// Still live.
		case orderStatusFilled:
			// It filled, so a cancel accepted for it applied to nothing and
			// must not be reported as having ended it.
			e.untrackOrderLocked(orderID)
		default:
			// Every remaining status ends the order without filling it in
			// full; validate() has already rejected anything unrecognized.
			// The reason is read before untracking, which clears the intent.
			reason := e.terminalReasonLocked(orderID, *update.Status)
			e.untrackOrderLocked(orderID)
			e.send(godex.OrderRejectedEvent{OrderID: orderID, Reason: reason})
		}
	}
	return nil
}

// resolveOrderIDLocked maps an order update onto an executor order id, by
// client order id when the venue echoes one and by venue oid otherwise.
func (e *Executor) resolveOrderIDLocked(update *wsOrderUpdate) (godex.OrderID, bool) {
	if update.Order.Cloid != nil {
		orderID := godex.OrderID(*update.Order.Cloid)
		if _, tracked := e.orders[orderID]; tracked {
			return orderID, true
		}
	}
	orderID, tracked := e.ordersByOid[*update.Order.Oid]
	return orderID, tracked
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
			e.logger.Error("hyperliquid account refresh failed", "error", err)
		}
	}()
}

// readAccount fetches the clearinghouse snapshot without interpreting it.
func (e *Executor) readAccount(ctx context.Context) (*clearinghouseState, error) {
	return postJSON[clearinghouseState](ctx, e.cfg.httpClient, e.cfg.restBaseURL,
		infoRequest{Type: infoTypeClearinghouseState, User: e.cfg.userAddress})
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
		coin:       e.cfg.coin,
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
				e.logger.Error("hyperliquid account poll failed", "error", err)
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
