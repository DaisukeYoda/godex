# Pre-Implementation Notes

Date: 2026-07-22

!!! note "Historical record"
    This note predates the shipped implementation. Current invariants are
    documented on the [Venue Contract](../concepts/contract.md) and
    [Market Data](../concepts/market-data.md) pages.

## Goal

Build a Go trading integration layer for dYdX v4, Lighter, and Hyperliquid.
The layer owns authenticated order placement, cancellation, account-state observation,
and venue-specific signing. Strategy and risk logic depend only on the normalized
interfaces and events.

This is not a generic exchange SDK. It is an application-facing adapter layer with a
small, safety-oriented contract.

## Proposed Boundary

```go
type VenueExecutor interface {
    Connect(ctx context.Context) error
    PlaceOrder(ctx context.Context, order NewOrder) (OrderAck, error)
    CancelOrder(ctx context.Context, id OrderID) error
    AccountEvents() <-chan AccountEvent
    Close() error
}
```

Common domain concepts:

- `NewOrder`: market, side, price, quantity, post-only, IOC, reduce-only, and client order ID.
- `OrderAck`: accepted, rejected, or unknown-submission outcome.
- `AccountEvent`: fill, position, margin, order rejection, connection state.
- `Position`: signed size, entry price where available, and venue observation timestamp.

Do not put venue-specific protocol fields into this contract. Examples include dYdX
`goodTilBlock`, Lighter per-key nonces, and Hyperliquid agent-wallet/EIP-712 details.
Each adapter owns those fields and validates its own invariants.

## SDK Inventory

### Lighter: Go

Candidate: [elliottech/lighter-go](https://github.com/elliottech/lighter-go)

- Published by Lighter/Elliot Tech as the reference implementation for transaction
  signing and hashing.
- Provides shared native signer libraries for macOS arm64, Linux amd64/arm64, and
  Windows amd64.
- Covers API key generation, auth token creation, create/modify/cancel/cancel-all
  order signatures, margin actions, transfers, and other signed transactions.
- Includes a small HTTP client for nonce retrieval and client validation.
- Does not provide the complete HTTP/WebSocket trading client surface. `lighter.Executor`
  must still implement REST submission, account WebSocket subscriptions, reconnects,
  state normalization, and nonce serialization.

Use it for signing rather than reimplementing Lighter transaction hashing.

### Hyperliquid: Go

Candidate: [sonirico/go-hyperliquid](https://github.com/sonirico/go-hyperliquid)

- Community-maintained, not an official Hyperliquid SDK.
- Declares REST API and WebSocket support.
- Must be evaluated on testnet before adoption for EIP-712 signing, agent-wallet use,
  order submission, cancellation, user fills, and user state subscriptions.

Other Go implementations exist, but no official Go SDK was listed in Hyperliquid's API
documentation at the time of this note.

### Hyperliquid: Rust

Candidate: [infinitefield/hypersdk](https://github.com/infinitefield/hypersdk)

- Listed in the [Hyperliquid API documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api)
  as a community Rust SDK.
- Relevant if Rust becomes the implementation language.

### dYdX v4: Go

- No official high-level Go trading SDK was identified.
- dYdX Chain itself is implemented in Go, but chain source is not a stable bot-client
  abstraction. Direct imports would couple the application to internal protocol APIs.
- The Go executor should use protocol protobufs and Cosmos signing/broadcast components
  only as necessary, behind `dydx.Executor`.
- Required dYdX functionality: permissioned trading wallet support, short-term order
  construction and signing, `goodTilBlock` calculation, cancellation, node submission,
  Indexer REST, and authenticated account WebSocket observation.

### dYdX v4: Rust

Candidate: [dydxprotocol/v4-clients/v4-client-rs](https://github.com/dydxprotocol/v4-clients/tree/main/v4-client-rs)

- Developed by Nethermind and included in the dYdX client repository.
- Provides Node and Indexer clients, WebSockets, request builders, automatic WebSocket
  connection support, and telemetry.
- The dYdX protocol also publishes Rust protobufs as
  [`dydx-proto`](https://github.com/dydxprotocol/v4-chain/tree/main/v4-proto-rs).

Rust reduces the dYdX implementation cost. Go keeps all three venue adapters in one
language but requires a purpose-built dYdX adapter.

## Safety Rules

- Keep master keys out of the trading process. Use venue-scoped trading keys or API keys.
- Keep SDK-specific signer instances inside each venue adapter.
- Assign and persist client order IDs before submission.
- Serialize Lighter transactions per API key and manage the next nonce locally.
- Never blindly retry an ambiguous order submission. Block dependent submissions, reconcile
  with order/account state, then recover or fail explicitly.
- Treat authenticated account fill events as the source of truth for fills and position
  changes. Order-book state is not evidence of execution.
- Validate REST and WebSocket payloads before mutating account state.
- Reconcile local order and position state from a verified initial snapshot at connection.
- Preserve venue sequence/block ordering rules and discard stale observations.

## What Should Be Unified

- Application order intent and normalized acknowledgement.
- Fills, positions, margin, rejections, and connection lifecycle events.
- Risk and unwind decisions that consume normalized account state.
- Reconciliation behavior for ambiguous submission outcomes.

## What Must Remain Venue-Specific

- Signing algorithms and key material.
- Lighter transaction nonce behavior and signer shared library.
- Hyperliquid EIP-712 action signatures and agent-wallet permissions.
- dYdX short-term order lifetime, `goodTilBlock`, Cosmos transaction encoding, and
  permissioned authenticator configuration.
- Exchange-specific order types and advanced features not required by the strategy.

## Recommended Delivery Order

1. Define normalized types and `VenueExecutor` with no live trading implementation.
2. Build testnet smoke tests for every venue: connect, account snapshot, distant post-only
   order, cancel, IOC order, fill observation, position observation, and reduce-only close.
3. Implement `lighter.Executor` using `lighter-go` and validate nonce/fault recovery.
4. Implement `hyperliquid.Executor` after the selected Go SDK passes the same testnet suite.
5. Implement `dydx.Executor` from the minimum required Go protocol components.
6. Run all adapters in observation or dry-run mode before enabling live strategy actions.

## Adoption Gates

An external SDK is only adopted after its testnet implementation proves all of the
following:

- Deterministic signing against venue-provided or existing trusted test vectors.
- Post-only rejection is observable and normalized.
- Cancel is idempotently recoverable after a timeout or disconnected response.
- IOC submission has an unambiguous reconciliation path.
- WebSocket reconnect produces no duplicated fill or position event.
- Local state converges to the venue snapshot after reconnect.

## Existing Omnibook Reference

The existing TypeScript code already uses this separation:

- `packages/executors/src/venue-executor.ts`: normalized execution contract.
- `packages/executors/src/dydx/`: dYdX signing, order submission, and normalization.
- `packages/executors/src/lighter/`: Lighter FFI signer, nonce manager, order submission,
  and normalization.
- `apps/trader/src/daemon.ts`: strategy orchestration, hedging, unwind, and risk behavior.

The Go version should preserve this direction of dependency rather than share raw SDK
types with strategy code.
