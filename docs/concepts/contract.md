# The Venue Contract

`VenueExecutor` is the single interface godex uses to speak to a perpetual
DEX — Lighter, dYdX v4, or Hyperliquid. One implementation of it exists per
venue, and each instance is scoped to exactly one market: authenticated order
placement, cancellation, and account-state observation all happen behind it,
with the venue's own wire protocol, signing scheme, and quirks kept entirely
inside the adapter.

The dependency direction is deliberate: strategy and risk code depends only
on godex's normalized types and events (`NewOrder`, `OrderAck`,
`AccountEvent`, and friends). It never depends on a venue package, and no
venue protocol detail — REST shapes, WS framing, signing formats, tick rules
— is allowed to leak across the interface. Swapping the venue underneath a
strategy should never require touching the strategy.

## The interface

```go
type VenueExecutor interface {
    VenueID() VenueID
    Connect(ctx context.Context) (ExecutionMetadata, error)
    PlaceOrder(ctx context.Context, order NewOrder) (OrderAck, error)
    CancelOrder(ctx context.Context, id OrderID) error
    AccountEvents() <-chan AccountEvent
    Close() error
}
```

## Design invariants

Every adapter — Lighter, dYdX v4, Hyperliquid — upholds the following. This
page is the canonical, detailed reference for them; treat it as complete.

### Maker orders are post-only

A taker-crossing rejection is a normal-path outcome: `PlaceOrder` returns
`AckRejected` (plus an `OrderRejectedEvent`), never an error. Strategy code
should not treat a crossing rejection as exceptional — it is an expected
branch of order placement, not a failure of the call itself.

### Fills come only from the authenticated account stream

Adapters never infer executions or positions from order-book state. Public
market data (order books, trades) is a separate concern from account state;
the two are never cross-wired to derive fills.

### Strict wire validation

Unexpected REST/WS payload shapes abort the connection instead of being
guessed at. Reconnection re-subscribes and re-converges from a verified
snapshot, so a malformed or unrecognized message never gets silently
coerced into a best-effort interpretation.

!!! warning "Ambiguous submissions halt, never retry blindly"
    When an outcome is unknown (e.g. a timeout), the adapter latches a
    fault:

    - The transaction is never resent.
    - Later submissions fail with `ErrTxOutcomeUnknown`.
    - The adapter reconciles with venue state before resuming.

    This is the core safety property of the contract: an adapter must never
    guess whether an order that timed out actually landed. Blind retries
    under ambiguity are how double-submission and phantom-order bugs enter
    a trading system, so the contract forbids them outright.

### Event ordering

`ConnectedEvent` / `DisconnectedEvent` alternate, including across internal
reconnects; all other events are emitted only in between them. The events
channel is buffered; when it is full, the adapter blocks rather than drops
events — consumers must consume promptly. The channel closes only after
`Close`.

### Quantization follows the venue's own rule

Quantization is not assumed to be a fixed tick size. Hyperliquid prices in
particular carry at most five significant figures *and* at most
`6 - szDecimals` decimals, so the increment is recomputed per order from the
price's magnitude rather than from a single cached tick.

### Risk metadata errs toward the account

Where a venue's maintenance margin varies with position size, the single
normalized fraction reports the strictest requirement, never the most
permissive. A strategy reading `MaintenanceMarginFraction` should be able to
trust it as a conservative bound, not an optimistic headline number.

### Venue lifetimes are made explicit

dYdX short-term orders expire on their own after roughly fifteen blocks; the
adapter reports that as an `OrderRejectedEvent` rather than letting a
strategy believe a quote is still live. Venue-specific order lifetime rules
are surfaced through the same normalized event vocabulary as any other order
termination, not left implicit.

!!! warning "An order finishes exactly once"
    Every end of an order — crossed, expired, cancelled — is one
    `OrderRejectedEvent`, whichever path observed it first.

    - An order that ended by a cancel the caller asked for carries
      `godex.ReasonCanceledByRequest` on every venue.
    - An adapter reports it only once the venue says the order ended:
      accepting a cancel means the request was valid, not that it applied,
      and a cancel accepted after the order already filled applied to
      nothing.
    - Lighter cannot observe a plain cancellation at all — see its package
      comment for how it reconciles that gap.

    Exactly-once termination is what lets a strategy treat
    `OrderRejectedEvent` as authoritative and final, with no risk of a
    duplicate or contradictory terminal event arriving later for the same
    order.

!!! note "Money is fixed-point"
    All prices and sizes use `decimal.Decimal` — a big-int mantissa plus
    scale, string-only construction, round half away from zero. No floats
    anywhere near order flow.

    This eliminates an entire class of rounding and representation bugs
    from price and size arithmetic; any conversion at the boundary of the
    library must go through `decimal`, never through a native float.
