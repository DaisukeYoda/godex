// Package godex is a trading integration layer for perpetual DEXes. It owns
// authenticated order placement, cancellation, account-state observation, and
// venue-specific signing behind a small, safety-oriented contract. Strategy
// and risk logic depend only on the normalized types and events in this
// package; venue adapters live in subpackages (lighter, ...).
//
// This is not a generic exchange SDK: the contract intentionally supports
// only what a post-only maker / IOC taker strategy needs.
package godex

import (
	"context"
	"errors"

	"github.com/DaisukeYoda/godex/decimal"
)

// VenueID identifies a supported venue.
type VenueID string

// VenueLighter is the Lighter (zkLighter) venue.
const VenueLighter VenueID = "lighter"

// ExecutionMetadata is venue market metadata resolved during Connect.
type ExecutionMetadata struct {
	// SizeStep is the venue's order size increment.
	SizeStep decimal.Decimal
	// MaintenanceMarginFraction is normalized to a decimal ratio. Venues
	// define it differently (decimal vs 1/10000 integer); each adapter
	// converts to a plain ratio.
	MaintenanceMarginFraction decimal.Decimal
}

// Sentinel errors shared by all venue adapters.
var (
	// ErrNotConnected is returned when an operation requires a connected
	// executor.
	ErrNotConnected = errors.New("godex: executor not connected")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("godex: executor closed")
	// ErrUnknownOrder is returned by CancelOrder for an ID the executor is
	// not tracking.
	ErrUnknownOrder = errors.New("godex: unknown order id")
	// ErrTxOutcomeUnknown reports that a submission's outcome could not be
	// determined (e.g. timeout). The executor latches this fault and blocks
	// further submissions until it reconciles with venue state; callers must
	// never blindly retry.
	ErrTxOutcomeUnknown = errors.New("godex: transaction outcome unknown")
)

// VenueExecutor is the normalized execution contract every venue adapter
// implements.
//
// Design invariants:
//   - Maker orders are always post-only. A taker-crossing rejection is a
//     normal-path outcome — PlaceOrder returns AckRejected (plus an
//     OrderRejectedEvent); it is never an error.
//   - REST and WebSocket payloads are validated strictly. Unexpected shapes
//     abort the connection instead of being guessed at (fail fast).
//   - Authenticated account-stream fills are the only source of truth for
//     executions. Adapters never infer fills or positions from book state.
//   - Price tick and size step rounding are the adapter's responsibility.
//
// Event ordering contract (AccountEvents):
//   - ConnectedEvent and DisconnectedEvent alternate, including across
//     internal reconnects.
//   - Other events are emitted only between a ConnectedEvent and the
//     following DisconnectedEvent.
type VenueExecutor interface {
	// VenueID identifies the venue this executor trades on.
	VenueID() VenueID

	// Connect loads venue market metadata, validates credentials, starts the
	// authenticated account stream, and emits a verified initial snapshot
	// (Connected, Position, Margin) before returning. Unsupported positions
	// or incomplete account state fail Connect.
	Connect(ctx context.Context) (ExecutionMetadata, error)

	// PlaceOrder rounds, signs, and submits the order.
	//
	// ctx cancellation is honored only until the transaction is dispatched;
	// once submission has started, PlaceOrder waits for the venue outcome
	// under the adapter's own request timeout — canceling mid-flight would
	// leave the submission ambiguous or orphan a live order.
	//
	// If a submission outcome is unknown, the adapter latches a fault: the
	// affected transaction is never retried and subsequent submissions fail
	// with ErrTxOutcomeUnknown until the adapter reconciles with venue state.
	PlaceOrder(ctx context.Context, order NewOrder) (OrderAck, error)

	// CancelOrder cancels a previously placed order by its executor-scoped
	// ID. Returns ErrUnknownOrder for IDs the executor is not tracking.
	CancelOrder(ctx context.Context, id OrderID) error

	// AccountEvents returns the executor's single account-event stream. The
	// channel is buffered (DefaultAccountEventBuffer); when it fills the
	// adapter blocks rather than dropping — a dropped fill would silently
	// corrupt position state. Consume promptly. The channel is closed only
	// after Close completes, so range termination means the executor is
	// terminal.
	AccountEvents() <-chan AccountEvent

	// Close tears the executor down: a final DisconnectedEvent is emitted
	// (if connected), then the event channel is closed. Close is terminal;
	// reconnecting means constructing a new executor.
	Close() error
}
