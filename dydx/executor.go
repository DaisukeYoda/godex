// Package dydx implements godex.VenueExecutor for dYdX v4.
//
// It places short-term orders: gas-free, matched synchronously in CheckTx (so a
// crossing post-only is rejected in the broadcast response rather than
// asynchronously), and valid for at most a handful of blocks. That last part is
// the venue's defining characteristic — a resting order here expires on its own
// after roughly fifteen blocks, which the adapter reports as an
// OrderRejectedEvent so a strategy never believes a quote is still live.
//
// Transactions are built, signed (SIGN_MODE_DIRECT secp256k1), and broadcast
// from a minimal vendored protobuf set rather than the dYdX chain module; see
// internal/pb. Market and account state come from the Indexer, while block
// height, account lookups, and broadcast go to a validator's CometBFT RPC.
package dydx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/DaisukeYoda/godex/internal/dedupe"
	"github.com/DaisukeYoda/godex/internal/ws"
)

const wsLabel = "dydx-account"

// orderRef is what the executor remembers about an order it placed.
type orderRef struct {
	clientID uint32
	// goodTilBlock is the block the order expires at, kept so an expiry
	// removal can be reported with the block that caused it.
	goodTilBlock uint32
}

// venueOrderRef maps one of the venue's order ids back to the caller's order id.
//
// The mapping deliberately outlives the order: a cancel and a fill can cross on
// chain, so a fill may reference an order the executor has already dropped, and
// losing the mapping would report that execution under a venue id the caller
// never saw. Entries are pruned by age instead, which bounds the map without
// giving up late attribution.
type venueOrderRef struct {
	orderID godex.OrderID
	at      time.Time
}

type accountInvalidObservation struct {
	err      error
	sequence int64
}

// Executor is the dYdX v4 implementation of godex.VenueExecutor.
type Executor struct {
	cfg    *resolvedConfig
	logger *slog.Logger

	events          chan godex.AccountEvent
	rejections      *dedupe.Set[godex.OrderID]
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	// opMu guards closed/connected and the in-flight operation accounting that
	// lets Close wait for every potential event emitter before closing the
	// events channel.
	opMu      sync.Mutex
	opWG      sync.WaitGroup
	closed    bool
	connected bool

	// txMu serializes sign → broadcast and guards the fault latch. Unlike a
	// nonce-ordered venue there is nothing to allocate: short-term orders skip
	// sequence validation, so the account number and sequence read at Connect
	// are reused for every transaction.
	txMu          sync.Mutex
	accountNumber uint64
	sequence      uint64
	txFault       error
	// txFaultUntilBlock is the block through which the ambiguous transaction's
	// order could still be live. Reconciliation waits for it.
	txFaultUntilBlock uint32
	faultTimer        *time.Timer
	acceptingTx       bool

	signer signer
	socket *ws.Socket
	height *heightTracker
	market *marketMeta

	// stateMu guards account-observation bookkeeping, order tracking, and event
	// emission (holding it through emission preserves per-batch ordering).
	stateMu              sync.Mutex
	observationSeq       int64
	lastStateObservation int64
	hasPositionSnapshot  bool
	hasMarginSnapshot    bool
	accountInvalid       *accountInvalidObservation
	orders               map[godex.OrderID]orderRef
	orderIDsByClientID   map[uint32]godex.OrderID
	orderIDsByVenueID    map[string]venueOrderRef
	clientIDCounter      uint32
	seenFills            map[string]time.Time
	// fillFloor is the instant through which the account's fills predate this
	// executor. Backfills never look past it, so a long history cannot be
	// mistaken for executions this executor missed.
	fillFloor    time.Time
	fillFloorSet bool

	// snapshotReady signals that a subscription snapshot has been applied.
	snapshotReady chan struct{}

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
		cfg:                resolved,
		logger:             resolved.logger,
		events:             make(chan godex.AccountEvent, godex.DefaultAccountEventBuffer),
		rejections:         dedupe.NewSet[godex.OrderID](dedupe.RejectionCapacity),
		lifecycleCtx:       lifecycleCtx,
		lifecycleCancel:    lifecycleCancel,
		orders:             make(map[godex.OrderID]orderRef),
		orderIDsByClientID: make(map[uint32]godex.OrderID),
		orderIDsByVenueID:  make(map[string]venueOrderRef),
		seenFills:          make(map[string]time.Time),
		snapshotReady:      make(chan struct{}, 1),
		// Randomly seeded so ids stay unique without persistence, across both
		// restarts and any two executors that happen to start together. A
		// wall-clock seed would collide outright for the latter. Collision is
		// only consequential while an order is live, which for a short-term
		// order is at most shortBlockWindow blocks.
		clientIDCounter: rand.Uint32(),
	}, nil
}

// VenueID implements godex.VenueExecutor.
func (e *Executor) VenueID() godex.VenueID { return godex.VenueDydx }

// AccountEvents implements godex.VenueExecutor.
func (e *Executor) AccountEvents() <-chan godex.AccountEvent { return e.events }

// Connect implements godex.VenueExecutor: it loads market metadata, verifies
// the signing key controls the configured address, reads the account number and
// sequence, establishes the block height, starts the account stream, and
// completes only after a verified snapshot has been emitted.
func (e *Executor) Connect(ctx context.Context) (godex.ExecutionMetadata, error) {
	// Connect is registered as an operation so a concurrent Close waits for it.
	// Otherwise Close could close the events channel while Connect is still
	// bringing the account stream up, and the stream's first ConnectedEvent
	// would be sent to a closed channel.
	if err := e.beginOp(); err != nil {
		return godex.ExecutionMetadata{}, err
	}
	defer e.endOp()

	e.opMu.Lock()
	if e.connected {
		e.opMu.Unlock()
		return godex.ExecutionMetadata{}, fmt.Errorf("dydx: executor already connected")
	}
	e.connected = true
	e.opMu.Unlock()

	// Close waits on this operation, so its own requests must be interruptible
	// by Close — a Connect blocked on an unresponsive venue would otherwise hang
	// Close indefinitely.
	connectCtx, cancelConnect := context.WithCancel(ctx)
	defer cancelConnect()
	stopCancelOnClose := context.AfterFunc(e.lifecycleCtx, cancelConnect)
	defer stopCancelOnClose()

	metadata, err := e.connect(connectCtx)
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

	market, err := e.loadMarketMeta(ctx)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.market = market

	sgnr, err := e.cfg.newSigner(e.cfg)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	// Orders are attributed to the configured address either way. Without an
	// authenticator the signing key must control that address, or the venue
	// would attribute the orders to somebody else's subaccount — or to nobody
	// at all. With an authenticator the scoped key is *expected* to be a
	// different key that the owner has authorized on chain, so requiring
	// equality here would reject exactly the setup that keeps a
	// withdrawal-capable key out of this process.
	derived := sgnr.address()
	switch {
	case e.cfg.credentials.AuthenticatorID != nil:
		e.logger.Info("dydx signing with a scoped authenticator key",
			"signingAddress", derived,
			"account", e.cfg.credentials.Address,
			"authenticatorID", *e.cfg.credentials.AuthenticatorID)
	case derived != e.cfg.credentials.Address:
		return godex.ExecutionMetadata{}, fmt.Errorf(
			"dydx: signing key controls %s, not the configured address %s",
			derived, e.cfg.credentials.Address)
	}
	e.signer = sgnr

	accountNumber, sequence, err := fetchAccountInfo(ctx, e.cfg.httpClient,
		e.cfg.rpcBaseURL, e.cfg.credentials.Address)
	if err != nil {
		return godex.ExecutionMetadata{}, err
	}
	e.txMu.Lock()
	e.accountNumber, e.sequence = accountNumber, sequence
	e.txMu.Unlock()

	e.height = newHeightTracker(func(ctx context.Context) (uint32, error) {
		return fetchHeight(ctx, e.cfg.httpClient, e.cfg.rpcBaseURL)
	}, e.cfg.heightStaleAfter, e.cfg.now)
	if err := e.height.refresh(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}

	socket := ws.New(wsLabel, e.cfg.indexerWSURL, e.cfg.reconnect, e.logger, ws.Handlers{
		OnOpen:    e.handleSocketOpen,
		OnMessage: e.handleSocketMessage,
		OnDown:    e.handleSocketDown,
	})
	if err := e.installSocket(socket); err != nil {
		return godex.ExecutionMetadata{}, err
	}
	if err := socket.Start(ctx); err != nil {
		return godex.ExecutionMetadata{}, err
	}

	if err := e.awaitInitialSnapshot(ctx); err != nil {
		_ = socket.Stop()
		return godex.ExecutionMetadata{}, err
	}

	if err := e.startPollers(); err != nil {
		_ = socket.Stop()
		return godex.ExecutionMetadata{}, err
	}

	e.txMu.Lock()
	e.acceptingTx = true
	e.txMu.Unlock()

	return godex.ExecutionMetadata{
		SizeStep:                  market.step,
		MaintenanceMarginFraction: market.maintenanceMarginFraction,
	}, nil
}

// installSocket publishes the account stream under the lifecycle lock. A Close
// that has already run wins: a late Connect must not resurrect a terminal
// executor, and Close reads this field to tear the stream down.
func (e *Executor) installSocket(socket *ws.Socket) error {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.closed {
		return godex.ErrClosed
	}
	e.socket = socket
	return nil
}

// startPollers launches the background loops under the lifecycle lock. Close
// sets closed before it waits on pollerWG, so registering the loops under the
// same lock rules out a poller starting after Close stopped waiting for them
// and then emitting into a closed channel.
func (e *Executor) startPollers() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.closed {
		return godex.ErrClosed
	}
	e.pollerWG.Add(3)
	go e.pingLoop()
	go e.heightPollLoop()
	go e.snapshotPollLoop()
	return nil
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
	if e.faultTimer != nil {
		e.faultTimer.Stop()
		e.faultTimer = nil
	}
	e.txMu.Unlock()
}

// loadMarketMeta fetches and fully parses the configured market. Every field
// the executor will need is resolved here, before Connect starts anything that
// would have to be torn down again.
func (e *Executor) loadMarketMeta(ctx context.Context) (*marketMeta, error) {
	response, err := fetchMarkets(ctx, e.cfg.httpClient, e.cfg.indexerRESTBaseURL)
	if err != nil {
		return nil, err
	}
	market, err := response.market(e.cfg.ticker)
	if err != nil {
		return nil, err
	}
	return newMarketMeta(market)
}

// awaitInitialSnapshot waits for the subscription snapshot the stream delivers
// after subscribing, which is what makes Connect's post-condition (a verified
// position and margin observation) true.
func (e *Executor) awaitInitialSnapshot(ctx context.Context) error {
	timer := time.NewTimer(e.cfg.txRequestTimeout)
	defer timer.Stop()
	select {
	case <-e.snapshotReady:
	case <-timer.C:
		return fmt.Errorf("dydx: no account snapshot within %s of subscribing", e.cfg.txRequestTimeout)
	case <-ctx.Done():
		return ctx.Err()
	case <-e.lifecycleCtx.Done():
		return godex.ErrClosed
	}
	e.stateMu.Lock()
	complete := e.hasPositionSnapshot && e.hasMarginSnapshot && e.accountInvalid == nil
	invalid := e.accountInvalid
	e.stateMu.Unlock()
	if !complete {
		if invalid != nil {
			return fmt.Errorf("dydx: initial account snapshot was rejected: %w", invalid.err)
		}
		return fmt.Errorf("dydx: initial account snapshot was not fully applied")
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

	// Unblock in-flight requests and any emitter waiting on a full events
	// channel, then tear the socket down (its Stop delivers the final
	// DisconnectedEvent via OnDown before returning).
	e.lifecycleCancel()
	e.opMu.Lock()
	socket := e.socket
	e.opMu.Unlock()
	if socket != nil {
		_ = socket.Stop()
	}
	e.pollerWG.Wait()
	e.opWG.Wait()

	close(e.events)
	return nil
}

// ForceReconnect force-closes the account stream so the automatic reconnect
// path (resubscription, snapshot re-convergence, fill backfill) runs. Used by
// the smoke-test reconnect gate.
func (e *Executor) ForceReconnect() error {
	e.opMu.Lock()
	socket := e.socket
	e.opMu.Unlock()
	if socket == nil {
		return godex.ErrNotConnected
	}
	socket.Abort()
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

func (e *Executor) endOp() { e.opWG.Done() }

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
		return godex.OrderAck{}, fmt.Errorf("dydx: account state is invalid: %w", invalid.err)
	}
	if order.Symbol != e.cfg.symbol {
		return godex.OrderAck{}, fmt.Errorf("dydx: executor is configured for %s, got %s",
			e.cfg.symbol, order.Symbol)
	}
	market := e.market
	if market == nil {
		return godex.OrderAck{}, godex.ErrNotConnected
	}
	// The chain only honors reduce-only on orders that cannot rest, and the
	// contract's post-only orders do rest. Refusing here beats submitting an
	// order whose reduce-only flag would be ignored.
	if order.ReduceOnly && order.Intent != godex.IntentIOC {
		return godex.OrderAck{}, fmt.Errorf(
			"dydx: reduce-only is only supported for IOC orders, got %s", order.Intent)
	}

	side, err := toSide(order.Side)
	if err != nil {
		return godex.OrderAck{}, err
	}
	timeInForce, err := toTimeInForce(order.Intent)
	if err != nil {
		return godex.OrderAck{}, err
	}

	price, err := godex.RoundPriceToTick(order.Price, market.tick, order.Side)
	if err != nil {
		return godex.OrderAck{}, err
	}
	var size decimal.Decimal
	if order.ReduceOnly {
		size, err = godex.QuantizeReduceOnlySize(order.Size, market.step)
	} else {
		// The venue publishes no maker minimum beyond the step itself.
		size, err = godex.QuantizeSize(order.Size, market.step, market.step)
	}
	if err != nil {
		return godex.OrderAck{}, err
	}

	quantums, err := market.toQuantums(size)
	if err != nil {
		return godex.OrderAck{}, err
	}
	subticks, err := market.toSubticks(price)
	if err != nil {
		return godex.OrderAck{}, err
	}
	goodTilBlock, err := e.height.goodTilBlock()
	if err != nil {
		return godex.OrderAck{}, err
	}

	orderID, clientID := e.allocateOrderID()
	params := placeOrderParams{
		orderIdentity: e.orderIdentity(clientID),
		side:          side,
		quantums:      quantums,
		subticks:      subticks,
		goodTilBlock:  goodTilBlock,
		timeInForce:   timeInForce,
		reduceOnly:    order.ReduceOnly,
	}

	// Track before submitting: an unknown outcome must leave a record of an
	// order that may be live.
	e.trackOrder(orderID, orderRef{clientID: clientID, goodTilBlock: goodTilBlock})
	result, err := e.submitTx(ctx, goodTilBlock, func(envelope txParams) ([]byte, error) {
		return e.signer.signPlaceOrder(params, envelope)
	})
	if err != nil {
		// An unknown outcome leaves the order possibly live, so it stays
		// tracked until reconciliation says otherwise.
		if !errors.Is(err, godex.ErrTxOutcomeUnknown) {
			e.untrackOrder(orderID)
		}
		return godex.OrderAck{}, err
	}

	switch result.outcome {
	case broadcastAccepted:
		return godex.OrderAck{
			OrderID: orderID, VenueID: godex.VenueDydx,
			Status: godex.AckSubmitted, Time: e.cfg.now(),
		}, nil
	case broadcastPostOnlyCrossed:
		// A post-only order that would take liquidity is a normal-path
		// rejection, never an error.
		e.untrackOrder(orderID)
		e.emitEvent(godex.OrderRejectedEvent{OrderID: orderID, Reason: result.log})
		return godex.OrderAck{
			OrderID: orderID, VenueID: godex.VenueDydx,
			Status: godex.AckRejected, Time: e.cfg.now(),
		}, nil
	default:
		e.untrackOrder(orderID)
		return godex.OrderAck{}, fmt.Errorf("dydx: order placement failed with code %d: %s",
			result.code, result.log)
	}
}

// CancelOrder implements godex.VenueExecutor.
func (e *Executor) CancelOrder(ctx context.Context, id godex.OrderID) error {
	if err := e.beginOp(); err != nil {
		return err
	}
	defer e.endOp()

	e.stateMu.Lock()
	ref, tracked := e.orders[id]
	e.stateMu.Unlock()
	if !tracked {
		return fmt.Errorf("%w: %s", godex.ErrUnknownOrder, id)
	}

	goodTilBlock, err := e.height.goodTilBlock()
	if err != nil {
		return err
	}
	// A short-term order past its expiry block is already gone; the chain would
	// reject a cancel for it. Canceling something the venue has forgotten is
	// exactly the case that must stay idempotent.
	if goodTilBlock-shortBlockForward >= ref.goodTilBlock {
		e.untrackOrder(id)
		return nil
	}

	params := cancelOrderParams{
		orderIdentity: e.orderIdentity(ref.clientID),
		goodTilBlock:  goodTilBlock,
	}
	result, err := e.submitTx(ctx, goodTilBlock, func(envelope txParams) ([]byte, error) {
		return e.signer.signCancelOrder(params, envelope)
	})
	if err != nil {
		return err
	}
	if result.outcome != broadcastAccepted {
		return fmt.Errorf("dydx: cancel failed with code %d: %s", result.code, result.log)
	}
	e.untrackOrder(id)
	// The chain accepted the cancel, so the order is finished and said so
	// here. Waiting for the Indexer's removal instead would report it only
	// when that update happens to arrive before the untrack above.
	e.emitEvent(godex.OrderRejectedEvent{OrderID: id, Reason: godex.ReasonCanceledByRequest})
	return nil
}

func (e *Executor) orderIdentity(clientID uint32) orderIdentity {
	return orderIdentity{
		address:          e.cfg.credentials.Address,
		subaccountNumber: e.cfg.credentials.SubaccountNumber,
		clientID:         clientID,
		clobPairID:       e.market.clobPairID,
	}
}

func (e *Executor) allocateOrderID() (godex.OrderID, uint32) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	clientID := e.clientIDCounter
	e.clientIDCounter++
	return godex.OrderID(strconv.FormatUint(uint64(clientID), 10)), clientID
}

func (e *Executor) trackOrder(id godex.OrderID, ref orderRef) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.orders[id] = ref
	e.orderIDsByClientID[ref.clientID] = id
}

func (e *Executor) untrackOrder(id godex.OrderID) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if ref, ok := e.orders[id]; ok {
		delete(e.orderIDsByClientID, ref.clientID)
	}
	delete(e.orders, id)
}

// --- transaction submission ---

// submitTx signs and broadcasts under txMu. A definite venue answer — accepted
// or rejected — is returned with a nil error. Anything that leaves the outcome
// unknown latches a fault instead: the transaction is never resent, later
// submissions fail with ErrTxOutcomeUnknown, and recovery reconciles against
// venue state before trading resumes.
func (e *Executor) submitTx(
	ctx context.Context,
	expiryBlock uint32,
	sign func(envelope txParams) ([]byte, error),
) (broadcastResult, error) {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if !e.acceptingTx {
		return broadcastResult{}, fmt.Errorf("dydx: executor is not accepting transactions: %w",
			godex.ErrNotConnected)
	}
	if err := e.assertTxCanStartLocked(ctx); err != nil {
		return broadcastResult{}, err
	}

	txBytes, err := sign(txParams{
		chainID:         e.cfg.chainID,
		accountNumber:   e.accountNumber,
		sequence:        e.sequence,
		authenticatorID: e.cfg.credentials.AuthenticatorID,
	})
	if err != nil {
		// Nothing was submitted, so this is an ordinary failure.
		return broadcastResult{}, err
	}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	defer cancel()
	result, err := broadcastTx(requestCtx, e.cfg.httpClient, e.cfg.rpcBaseURL, txBytes)
	if err != nil {
		if e.lifecycleCtx.Err() != nil {
			return broadcastResult{}, fmt.Errorf("dydx: transaction lifecycle ended: %w", e.lifecycleCtx.Err())
		}
		return broadcastResult{}, e.latchTxFaultLocked(err, expiryBlock)
	}
	return result, nil
}

func (e *Executor) assertTxCanStartLocked(ctx context.Context) error {
	if err := e.lifecycleCtx.Err(); err != nil {
		return fmt.Errorf("dydx: transaction lifecycle ended: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dydx: order submission canceled before broadcast: %w", err)
	}
	if e.txFault != nil {
		return e.txFault
	}
	return nil
}

// latchTxFaultLocked records an unknown-outcome fault and schedules recovery.
// The faulted transaction itself is never resent. untilBlock is the block
// through which the ambiguous transaction could still take effect.
func (e *Executor) latchTxFaultLocked(cause error, untilBlock uint32) error {
	if e.txFault == nil {
		e.txFault = fmt.Errorf("%w; reconciling with venue state: %v", godex.ErrTxOutcomeUnknown, cause)
		e.txFaultUntilBlock = untilBlock
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

// recoverTxFault reconciles local state with the venue after an unknown
// submission outcome.
//
// It deliberately waits for the ambiguous transaction's expiry block before
// concluding anything. The Indexer is eventually consistent, so an order the
// chain accepted seconds ago may simply not be indexed yet — reading "not open"
// then and declaring the order rejected would be a guess, and for an IOC that
// filled it would be the wrong guess. Once the expiry block has passed the order
// cannot be live under any interpretation, so absence is conclusive; the fill
// backfill that runs first ensures any execution it did produce is reported
// before the order is written off.
func (e *Executor) recoverTxFault() {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	e.faultTimer = nil
	if e.txFault == nil || !e.acceptingTx || e.lifecycleCtx.Err() != nil {
		return
	}

	height, err := e.height.current()
	if err != nil || height <= e.txFaultUntilBlock {
		// Either the chain tip is unknown or the order could still be live.
		// Both mean waiting, not resuming.
		e.scheduleFaultRecoveryLocked()
		return
	}

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	err = e.reconcileAfterUnknownOutcome(requestCtx)
	cancel()
	if err != nil {
		if e.acceptingTx && e.lifecycleCtx.Err() == nil {
			e.scheduleFaultRecoveryLocked()
		}
		return
	}
	e.txFault = nil
	e.txFaultUntilBlock = 0
	e.logger.Info("dydx tx fault recovered", "reconciledThroughBlock", height)
}

// reconcileAfterUnknownOutcome reports any execution the ambiguous transaction
// produced, then writes off the orders the venue no longer knows about.
func (e *Executor) reconcileAfterUnknownOutcome(ctx context.Context) error {
	if err := e.backfillFills(ctx); err != nil {
		return err
	}
	return e.reconcileOrders(ctx)
}

// reconcileOrders drops tracked orders the venue no longer reports as open. An
// order that vanished while its outcome was unknown is reported as a rejection
// so the strategy stops waiting on it.
func (e *Executor) reconcileOrders(ctx context.Context) error {
	response, err := fetchOpenOrders(ctx, e.cfg.httpClient, e.cfg.indexerRESTBaseURL,
		e.cfg.credentials.Address, e.cfg.credentials.SubaccountNumber)
	if err != nil {
		return err
	}
	open := make(map[uint32]struct{}, len(response.Orders))
	for i := range response.Orders {
		clientID, err := clientIDOf(&response.Orders[i])
		if err != nil {
			return err
		}
		open[clientID] = struct{}{}
	}

	e.stateMu.Lock()
	var stale []godex.OrderID
	for id, ref := range e.orders {
		if _, live := open[ref.clientID]; !live {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		ref := e.orders[id]
		delete(e.orders, id)
		delete(e.orderIDsByClientID, ref.clientID)
		e.send(godex.OrderRejectedEvent{
			OrderID: id,
			Reason: fmt.Sprintf(
				"not open at the venue past expiry block %d after an unknown submission outcome",
				ref.goodTilBlock),
		})
	}
	e.stateMu.Unlock()
	return nil
}

// --- account stream ---

func (e *Executor) handleSocketOpen() error {
	subscribe, err := json.Marshal(map[string]string{
		"type":    "subscribe",
		"channel": subaccountsChannel,
		"id":      e.cfg.subaccountID(),
	})
	if err != nil {
		return err
	}
	if err := e.socket.Send(string(subscribe)); err != nil {
		return err
	}
	e.emitEvent(godex.ConnectedEvent{VenueID: godex.VenueDydx})
	return nil
}

func (e *Executor) handleSocketDown() {
	e.emitEvent(godex.DisconnectedEvent{VenueID: godex.VenueDydx})
}

func (e *Executor) handleSocketMessage(raw []byte) error {
	message, err := decodeSubaccountWsMessage(raw)
	if err != nil {
		return err
	}
	switch message.Type {
	case wsTypeConnected, wsTypePong, wsTypeUnsubscribed:
		return nil
	case wsTypeSubscribed:
		return e.handleSubscribed(message.Contents)
	case wsTypeChannelData:
		return e.handleChannelData(message.Contents)
	default:
		return fmt.Errorf("dydx: unhandled ws message type %q", message.Type)
	}
}

// handleSubscribed applies the snapshot delivered on every (re)subscription.
//
// The snapshot carries current state but no execution history, so a fill that
// landed while the stream was down would otherwise be lost — its effect visible
// in the position, its FillEvent never emitted. Backfilled fills are therefore
// emitted first, before the position they explain.
func (e *Executor) handleSubscribed(contents *subaccountContents) error {
	if contents.Subaccount == nil {
		return fmt.Errorf("dydx: subscription snapshot carries no subaccount state")
	}
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	e.stateMu.Unlock()

	e.indexOrders(contents.Orders)

	requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
	err := e.backfillFills(requestCtx)
	cancel()
	if err != nil {
		return err
	}

	events, err := normalizeSnapshot(contents.Subaccount, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return err
	}
	applied := e.applyStateEvents(events, sequence)
	e.clearAccountInvalid(sequence, applied)

	select {
	case e.snapshotReady <- struct{}{}:
	default:
	}
	return nil
}

// handleChannelData applies an incremental update. Order removals and fills are
// always applied; position updates are applied only when the venue priced them
// plausibly, and otherwise trigger a snapshot re-read rather than a position
// godex would not stand behind.
func (e *Executor) handleChannelData(contents *subaccountContents) error {
	e.indexOrders(contents.Orders)

	// Fills first: a frame can carry both an execution and the terminal order
	// update it caused, and the execution is the reason for the ending.
	//
	// Across frames the venue's order stands. It reports an IOC remainder's
	// cancellation before the execution that remainder is left over from, so a
	// rejection can precede its own fill; godex.OrderRejectedEvent documents
	// that, and the fill stays attributable because the venue-id mapping
	// outlives the order.
	for i := range contents.Fills {
		if err := e.emitFill(&contents.Fills[i]); err != nil {
			return err
		}
	}
	e.applyOrderUpdates(contents.Orders)

	entry := findPosition(contents.PerpetualPositions, e.cfg.ticker)
	if entry == nil {
		return nil
	}
	if !entry.complete() || entry.entryPriceUnsettled() {
		e.requestSnapshotRefresh()
		return nil
	}
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	e.stateMu.Unlock()
	position, err := toPosition(entry, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return err
	}
	e.applyStateEvents([]godex.AccountEvent{godex.PositionEvent{Position: position}}, sequence)
	return nil
}

// indexOrders records the venue's own order ids for the orders this executor
// placed, so its fills can be attributed back to the caller's order id.
func (e *Executor) indexOrders(orders ordersResponse) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	for i := range orders {
		update := &orders[i]
		if update.ID == nil {
			continue
		}
		clientID, err := clientIDOf(update)
		if err != nil {
			continue
		}
		if id, ok := e.orderIDsByClientID[clientID]; ok {
			e.rememberVenueOrderLocked(*update.ID, id)
		}
	}
}

// applyOrderUpdates retires tracked orders the venue reports as finished.
//
// A removal is reported as a rejection — a short-term order reaching
// good_til_block lands here too, since the contract has no expiry event and
// "rejected" carries the right meaning: the order is gone and will never fill.
// A fully filled order is retired silently, because its fills have already been
// delivered and a rejection would contradict them. Either way the order stops
// being tracked, which is what keeps the tables from growing for the life of the
// process.
func (e *Executor) applyOrderUpdates(orders ordersResponse) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	for i := range orders {
		update := &orders[i]
		removed, filled := isRemoval(update), isFilled(update)
		if !removed && !filled {
			continue
		}
		clientID, err := clientIDOf(update)
		if err != nil {
			continue
		}
		id, tracked := e.orderIDsByClientID[clientID]
		if !tracked {
			continue
		}
		ref := e.orders[id]
		delete(e.orders, id)
		delete(e.orderIDsByClientID, clientID)
		if removed {
			e.send(godex.OrderRejectedEvent{OrderID: id, Reason: rejectionReason(update, ref)})
		}
	}
}

// rememberVenueOrderLocked records a venue order id, pruning entries too old to
// still receive a fill so the mapping stays bounded.
func (e *Executor) rememberVenueOrderLocked(venueOrderID string, id godex.OrderID) {
	now := e.cfg.now()
	e.orderIDsByVenueID[venueOrderID] = venueOrderRef{orderID: id, at: now}
	for seen, ref := range e.orderIDsByVenueID {
		if now.Sub(ref.at) > venueOrderMappingTTL {
			delete(e.orderIDsByVenueID, seen)
		}
	}
}

// emitFill delivers a fill exactly once. The Indexer's own fill id is the
// duplicate-suppression key, which is what makes the reconnect backfill safe to
// run unconditionally.
func (e *Executor) emitFill(entry *fill) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	if _, seen := e.seenFills[*entry.ID]; seen {
		return nil
	}
	// A subaccount can trade several markets at once. FillEvent has no market
	// field, so emitting a foreign fill would present it under this executor's
	// symbol — indistinguishable from one of its own executions. It is recorded
	// as seen so later backfills stop re-examining it.
	if entry.marketTicker() != e.cfg.ticker {
		e.rememberFillLocked(*entry.ID)
		return nil
	}

	orderID := godex.OrderID("")
	if entry.OrderID != nil {
		if ref, ok := e.orderIDsByVenueID[*entry.OrderID]; ok {
			orderID = ref.orderID
		} else {
			// A fill on an order this executor did not place — another client
			// on the same subaccount. It still moves the position, so it is
			// reported rather than dropped, under the venue's own order id.
			orderID = godex.OrderID(*entry.OrderID)
			e.logger.Warn("dydx fill for an order this executor did not place",
				"venueOrderID", *entry.OrderID)
		}
	}
	event, err := toFill(entry, orderID)
	if err != nil {
		return err
	}
	e.rememberFillLocked(*entry.ID)
	e.send(event)
	return nil
}

// rememberFillLocked records a fill id and prunes expired entries so the cache
// cannot grow without bound in a long-running process.
func (e *Executor) rememberFillLocked(id string) {
	now := e.cfg.now()
	e.seenFills[id] = now
	for seen, at := range e.seenFills {
		if now.Sub(at) > fillDedupTTL {
			delete(e.seenFills, seen)
		}
	}
}

// backfillFills reconciles the executor's fill history with the venue's.
//
// On the first subscription it only establishes a floor: the account's existing
// fills predate this executor and are absorbed without being emitted, because a
// consumer accumulating FillEvents would otherwise book yesterday's executions
// as new ones. On every later subscription the same read closes the gap left by
// the disconnect — a fill that landed while the stream was down is visible in
// the fresh position but would never arrive as an event.
//
// A long outage can produce more fills than one page holds, so the read pages
// backwards until it reaches known ground. Running out of pages is reported as
// an error rather than treated as convergence: silently keeping the newest N
// would drop executions while claiming the account is reconciled.
func (e *Executor) backfillFills(ctx context.Context) error {
	e.stateMu.Lock()
	establishing := !e.fillFloorSet
	floor := e.fillFloor
	e.stateMu.Unlock()

	if establishing {
		return e.establishFillFloor(ctx)
	}
	return e.emitMissedFills(ctx, floor)
}

// establishFillFloor absorbs the account's existing fills without emitting them.
// One page fixes the floor; anything older is excluded by that floor rather than
// by having been enumerated, so a long trading history costs one request.
func (e *Executor) establishFillFloor(ctx context.Context) error {
	response, err := fetchFills(ctx, e.cfg.httpClient, e.cfg.indexerRESTBaseURL,
		e.cfg.credentials.Address, e.cfg.credentials.SubaccountNumber, fillBackfillLimit, "")
	if err != nil {
		return err
	}
	fills := *response.Fills

	newest := time.Time{}
	for i := range fills {
		// The floor is only as trustworthy as the timestamps it is built from,
		// so an unparseable one fails here exactly as it would during a
		// backfill rather than silently lowering the floor.
		createdAt, err := time.Parse(time.RFC3339, *fills[i].CreatedAt)
		if err != nil {
			return fmt.Errorf("dydx: fill createdAt %q: %w", *fills[i].CreatedAt, err)
		}
		if createdAt.After(newest) {
			newest = createdAt
		}
	}

	e.stateMu.Lock()
	for i := range fills {
		e.rememberFillLocked(*fills[i].ID)
	}
	e.fillFloor, e.fillFloorSet = newest, true
	e.stateMu.Unlock()

	if len(fills) > 0 {
		e.logger.Info("dydx ignoring fills that predate this executor",
			"count", len(fills), "through", newest)
	}
	return nil
}

// emitMissedFills reports executions that landed while the stream was down,
// paging back until it reaches ground the executor already knows.
func (e *Executor) emitMissedFills(ctx context.Context, floor time.Time) error {
	var missed []*fill
	createdBeforeOrAt := ""
	converged := false
	for page := 0; page < maxFillBackfillPages && !converged; page++ {
		response, err := fetchFills(ctx, e.cfg.httpClient, e.cfg.indexerRESTBaseURL,
			e.cfg.credentials.Address, e.cfg.credentials.SubaccountNumber,
			fillBackfillLimit, createdBeforeOrAt)
		if err != nil {
			return err
		}
		fills := *response.Fills
		if len(fills) == 0 {
			converged = true
			break
		}
		oldest, reachedKnown, err := e.collectMissedFills(fills, floor, &missed)
		if err != nil {
			return err
		}
		// A short page means the venue has nothing older to offer.
		converged = reachedKnown || len(fills) < fillBackfillLimit
		createdBeforeOrAt = oldest
	}
	if !converged {
		return fmt.Errorf(
			"dydx: fill backfill did not reach known history within %d pages of %d; "+
				"refusing to report partial convergence",
			maxFillBackfillPages, fillBackfillLimit)
	}

	// Pages arrive newest first; emit oldest first so the stream reads
	// chronologically.
	for i := len(missed) - 1; i >= 0; i-- {
		if err := e.emitFill(missed[i]); err != nil {
			return err
		}
	}
	return nil
}

// collectMissedFills appends the not-yet-seen fills of one page to missed. It
// reports the page's oldest timestamp and whether the page reached ground the
// executor already knows — either fills it has emitted or fills at or below the
// floor established at connect.
func (e *Executor) collectMissedFills(
	fills []fill,
	floor time.Time,
	missed *[]*fill,
) (oldest string, reachedKnown bool, err error) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	for i := range fills {
		entry := &fills[i]
		createdAt, parseErr := time.Parse(time.RFC3339, *entry.CreatedAt)
		if parseErr != nil {
			return "", false, fmt.Errorf("dydx: fill createdAt %q: %w", *entry.CreatedAt, parseErr)
		}
		if !floor.IsZero() && !createdAt.After(floor) {
			reachedKnown = true
			continue
		}
		if _, seen := e.seenFills[*entry.ID]; seen {
			reachedKnown = true
			continue
		}
		*missed = append(*missed, entry)
	}
	return *fills[len(fills)-1].CreatedAt, reachedKnown, nil
}

// requestSnapshotRefresh re-reads account state out of band, used when an
// incremental update is too sparse to act on.
func (e *Executor) requestSnapshotRefresh() {
	if e.beginOp() != nil {
		return
	}
	// Reserved here rather than inside the goroutine, on the stream reader's
	// own goroutine: every update the reader handles after this call must
	// outrank the re-read. Reserving in the goroutine leaves that to the
	// scheduler, and losing the race lets a REST response describing an older
	// state overwrite a newer stream update — the state the re-read was
	// prompted by, in the case that matters.
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	e.stateMu.Unlock()

	go func() {
		defer e.endOp()
		if err := e.refreshSnapshotAt(e.lifecycleCtx, sequence); err != nil && e.lifecycleCtx.Err() == nil {
			e.logger.Error("dydx account snapshot refresh failed", "error", err)
		}
	}()
}

// refreshSnapshot reads account state over REST and emits position and margin
// observations. The observation sequence is reserved before the request so a
// slow response cannot overwrite newer stream state.
func (e *Executor) refreshSnapshot(ctx context.Context) error {
	e.stateMu.Lock()
	sequence := e.nextObservationSeqLocked()
	e.stateMu.Unlock()
	return e.refreshSnapshotAt(ctx, sequence)
}

// refreshSnapshotAt is refreshSnapshot under an already-reserved observation
// sequence, for callers that must order the re-read against stream updates they
// have not handled yet.
func (e *Executor) refreshSnapshotAt(ctx context.Context, sequence int64) error {
	response, err := e.readSettledSubaccount(ctx)
	if err != nil {
		return err
	}
	events, err := normalizeSnapshot(response.Subaccount, e.normalizeContext())
	if err != nil {
		e.setAccountInvalid(err, sequence)
		return err
	}
	applied := e.applyStateEvents(events, sequence)
	e.clearAccountInvalid(sequence, applied)
	return nil
}

// readSettledSubaccount reads the account snapshot, re-reading while it still
// carries a position with size at a zero entry price. The Indexer serves REST
// from the same state the stream publishes, so a re-read prompted by that
// transient can arrive before the venue has priced the position — believing the
// first answer would put the very number the re-read exists to avoid back into
// the stream.
//
// Bounded, and the last read is used either way. A zero that repeats across
// reads this far apart is the venue's answer rather than a moment in flight,
// and refusing it indefinitely would strand a position that really is unpriced.
// If the stream's own correction lands while this is confirming, it carries a
// later observation sequence and wins.
func (e *Executor) readSettledSubaccount(ctx context.Context) (*subaccountResponse, error) {
	for attempt := 1; ; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, e.cfg.txRequestTimeout)
		response, err := fetchSubaccount(requestCtx, e.cfg.httpClient, e.cfg.indexerRESTBaseURL,
			e.cfg.credentials.Address, e.cfg.credentials.SubaccountNumber)
		cancel()
		if err != nil {
			return nil, err
		}
		entry := findPosition(response.Subaccount.OpenPerpetualPositions, e.cfg.ticker)
		if entry == nil || !entry.entryPriceUnsettled() {
			return response, nil
		}
		if attempt == positionPriceReads {
			e.logger.Warn("dydx reports a position with size at a zero entry price; publishing it",
				"market", e.cfg.ticker, "size", *entry.Size, "reads", attempt)
			return response, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(positionPriceRereadDelay):
		}
	}
}

// applyStateEvents emits a batch, dropping stale position and margin
// observations. Fills and rejections are never dropped. Reports whether fresh
// state was applied.
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
		switch event.(type) {
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
		}
		e.send(event)
	}
	return hasStateEvent && !staleState
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
		ticker:     e.cfg.ticker,
		receivedAt: e.cfg.now(),
	}
}

// --- pollers ---

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

// heightPollLoop keeps the block height fresh enough to derive good_til_block.
// A single failed poll is transient; the tracker's staleness rule is what stops
// submissions.
func (e *Executor) heightPollLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(e.cfg.heightPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.lifecycleCtx.Done():
			return
		case <-ticker.C:
			requestCtx, cancel := context.WithTimeout(e.lifecycleCtx, e.cfg.txRequestTimeout)
			err := e.height.refresh(requestCtx)
			cancel()
			if err != nil && e.lifecycleCtx.Err() == nil {
				e.logger.Warn("dydx block height poll failed", "error", err)
			}
		}
	}
}

// snapshotPollLoop refreshes equity and position over REST. The account stream
// reports equity only in its subscription snapshot, so margin would otherwise
// go stale for the life of a connection.
func (e *Executor) snapshotPollLoop() {
	defer e.pollerWG.Done()
	ticker := time.NewTicker(snapshotPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.lifecycleCtx.Done():
			return
		case <-ticker.C:
			if !e.socket.IsOpen() {
				continue
			}
			if err := e.refreshSnapshot(e.lifecycleCtx); err != nil && e.lifecycleCtx.Err() == nil {
				e.logger.Error("dydx account snapshot poll failed", "error", err)
			}
		}
	}
}

// --- event emission ---

// send delivers an event; callers hold stateMu so batches stay ordered. A full
// buffer blocks (dropping a fill would silently corrupt position state) until
// the consumer drains or the executor closes. The non-blocking first attempt
// guarantees delivery whenever the buffer has room — in particular the final
// DisconnectedEvent during Close, which runs with the lifecycle context already
// canceled.
//
// One order's rejection is reported at most once. Two paths observe the same
// outcome — the CheckTx response to the submission and the Indexer's order
// update — and neither is ordered against the other, so the losing path is
// dropped here rather than at each call site. The dropped copy carries no
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

// emitEvent delivers a single event outside a normalization batch.
func (e *Executor) emitEvent(event godex.AccountEvent) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.send(event)
}
