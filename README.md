<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo.svg" alt="godex" width="340">
  </picture>

  <p><strong>Go trading integration layer for perpetual DEXes</strong><br>
  Lighter (zkLighter) · dYdX v4 · Hyperliquid</p>

  <p>
    <a href="https://github.com/DaisukeYoda/godex/actions/workflows/ci.yml"><img src="https://github.com/DaisukeYoda/godex/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://pkg.go.dev/github.com/DaisukeYoda/godex"><img src="https://pkg.go.dev/badge/github.com/DaisukeYoda/godex.svg" alt="Go Reference"></a>
    <a href="https://github.com/DaisukeYoda/godex/tags"><img src="https://img.shields.io/github/v/tag/DaisukeYoda/godex?label=release&color=5A67D8" alt="Release"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white" alt="Go 1.24"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-97CA00" alt="License: MIT"></a>
  </p>

  <p>
    <a href="https://daisukeyoda.github.io/godex/">Documentation</a> ·
    <a href="https://daisukeyoda.github.io/godex/getting-started/">Getting Started</a> ·
    <a href="https://daisukeyoda.github.io/godex/concepts/contract/">The Venue Contract</a> ·
    <a href="https://daisukeyoda.github.io/godex/concepts/market-data/">Market Data</a> ·
    <a href="https://daisukeyoda.github.io/godex/guides/security/">Security</a>
  </p>
</div>

---

godex owns authenticated order placement, cancellation, account-state
observation, and venue-specific signing behind a small, safety-oriented
contract. Strategy and risk logic depend only on the normalized types and
events; venue protocol details never leak out.

This is **not** a generic exchange SDK. The contract intentionally supports
exactly what a post-only maker / IOC taker strategy needs.

**Status: pre-release.** All three adapters are implemented with full unit
suites, and all three have passed the full testnet
[adoption-gate run](https://daisukeyoda.github.io/godex/guides/smoke-testing/).

| Venue | Execution | Market data | Adoption gate |
| :--- | :---: | :---: | :---: |
| **Lighter** (zkLighter) | ✅ | ✅ | ✅ passed |
| **dYdX v4** | ✅ | ✅ | ✅ passed |
| **Hyperliquid** | ✅ | — | ✅ passed |

## Install

```sh
go get github.com/DaisukeYoda/godex
```

## Quickstart (Hyperliquid, testnet)

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/DaisukeYoda/godex"
    "github.com/DaisukeYoda/godex/decimal"
    "github.com/DaisukeYoda/godex/hyperliquid"
)

func main() {
    exec, err := hyperliquid.New(hyperliquid.Config{
        Credentials: hyperliquid.Credentials{
            // The account that holds the position — the master account when an
            // API wallet signs for it.
            AccountAddress: os.Getenv("HYPERLIQUID_ACCOUNT_ADDRESS"), // 0x...
            APIPrivateKey:  os.Getenv("HYPERLIQUID_API_PRIVATE_KEY"), // API (agent) wallet key
        },
        Symbol:  "ETH-PERP",
        Coin:    "ETH", // venue perp name
        Network: hyperliquid.Testnet,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer exec.Close()

    go func() {
        for event := range exec.AccountEvents() {
            log.Printf("%#v", event)
        }
    }()

    ctx := context.Background()
    meta, err := exec.Connect(ctx)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("sizeStep=%s mmf=%s", meta.SizeStep, meta.MaintenanceMarginFraction)

    ack, err := exec.PlaceOrder(ctx, godex.NewOrder{
        Symbol: "ETH-PERP",
        Side:   godex.SideBuy,
        Price:  decimal.MustFromString("1000.0", 1), // quantized per the venue's rule
        Size:   decimal.MustFromString("0.010", 3),
        Intent: godex.IntentPostOnly,
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("ack: %+v", ack)
}
```

Every adapter follows this same shape — construct a venue `Config`, `Connect`,
consume `AccountEvents`, place and cancel orders — only the credentials and
market identifiers differ. Per-venue setup (Lighter, dYdX) and the
venue-specific behavior notes live in
[Getting Started](https://daisukeyoda.github.io/godex/getting-started/).

## The contract

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

Design invariants every adapter upholds:

- **Maker orders are post-only.** A taker-crossing rejection is a normal-path
  outcome: PlaceOrder returns `AckRejected` (plus an `OrderRejectedEvent`),
  never an error.
- **Fills come only from the authenticated account stream.** Adapters never
  infer executions or positions from order-book state.
- **Strict wire validation.** Unexpected REST/WS payload shapes abort the
  connection instead of being guessed at; reconnection re-subscribes and
  re-converges from a verified snapshot.
- **Ambiguous submissions halt, never retry blindly.** When an outcome is
  unknown (e.g. timeout), the adapter latches a fault: the transaction is
  never resent, later submissions fail with `ErrTxOutcomeUnknown`, and the
  adapter reconciles with venue state before resuming.
- **Event ordering.** `ConnectedEvent`/`DisconnectedEvent` alternate
  (including across internal reconnects); all other events are emitted only
  in between. The events channel is buffered; when full, the adapter blocks
  rather than drops — consume promptly. The channel closes only after
  `Close`.
- **Quantization follows the venue's own rule.** Hyperliquid prices are not a
  fixed tick: they carry at most five significant figures *and* at most
  `6 - szDecimals` decimals, so the increment is recomputed per order from the
  price's magnitude.
- **Risk metadata errs toward the account.** Where a venue's maintenance margin
  varies with position size, the single normalized fraction reports the
  strictest requirement, never the most permissive.
- **Venue lifetimes are made explicit.** dYdX short-term orders expire on
  their own after roughly fifteen blocks; the adapter reports that as an
  `OrderRejectedEvent` rather than letting a strategy believe a quote is
  still live.
- **An order finishes exactly once.** Every end of an order — crossed,
  expired, cancelled — is one `OrderRejectedEvent`, whichever path observed
  it first. An order that ended by a cancel the caller asked for carries
  `godex.ReasonCanceledByRequest` on every venue, and an adapter reports it
  only once the venue says the order ended: accepting a cancel means the
  request was valid, not that it applied, and one accepted as the order
  filled applied to nothing. Lighter cannot observe a plain cancellation at
  all — see its package comment.
- **Money is fixed-point.** All prices/sizes use `decimal.Decimal` (big-int
  mantissa + scale, string-only construction, round half away from zero).
  No floats anywhere near order flow.

## Market data (public, read-only)

Alongside execution, godex normalizes the public market data a maker/taker
strategy consumes — order books and funding — behind two contracts, one
streamed and one polled:

```go
type MarketStream interface {          // WS order-book stream
    VenueID() VenueID
    Start(ctx context.Context) error
    Events() <-chan MarketEvent        // BookSnapshot | MarketConnected | MarketDisconnected
    Close() error
}

type MarketDataClient interface {      // REST polls
    VenueID() VenueID
    FundingRate(ctx context.Context) (FundingRate, error)
    MarketStats(ctx context.Context) (MarketStats, error)
}
```

Like executors, one stream or client serves one market. Both are available for
Lighter (`lighter.NewMarketStream` / `lighter.NewMarketData`) and dYdX
(`dydx.NewMarketStream` / `dydx.NewMarketData`); they need no credentials.

Invariants:

- **A crossed book is never emitted.** Delta-driven crossings on dYdX are
  uncrossed by treating the later update as the freshest state (the official
  client's interpretation); a crossed snapshot resubscribes the market, and on
  Lighter a crossed book suppresses emits and rebuilds the connection past a
  short grace.
- **Sequence integrity is proven, not assumed.** dYdX `message_id` is tracked
  through a contiguous watermark that tolerates cross-channel reordering but
  aborts on true gaps and duplicates; Lighter updates must chain
  `begin_nonce` → `nonce` exactly. Either failure rebuilds the connection and
  the book — no gap is ever papered over.
- **Snapshot/delta reassembly is internal.** Consumers always receive full,
  sorted book snapshots.
- **Funding rates are normalized** to a plain per-interval decimal at
  `FundingRateScale` with the venue's sign convention folded in (positive =
  longs pay shorts), so rates from different venues subtract cleanly.

dYdX also exposes `dydx.FetchFundingPayments` (the Indexer's per-account
settled funding history, public REST) and `dydx.KeyFromMnemonic`, which
derives the account key at the Cosmos HD path `m/44'/118'/0'/0/0` exactly as
the official clients do — pinned in tests against `@dydxprotocol/v4-client-js`.

To watch live market data without touching execution:

```sh
go run ./cmd/godex-smoke -market-watch -venue dydx -network mainnet \
  -ticker SOL-USD -symbol SOL-PERP -price-scale 4 -size-scale 3 -watch-duration 30s
```

## Security

- Credentials are passed in as struct fields; the library never reads
  environment variables or files. `cmd/godex-smoke` reads env vars and fails
  fast when they are missing.
- Use **venue-scoped, trading-only API keys**. Withdrawal-capable master keys
  (L1 wallets) must never reach a trading process. dYdX has no separate API
  key, so register a dedicated key as a scoped on-chain authenticator
  (`accountplus`) and name it in `Credentials.AuthenticatorID`. The chain takes
  one authenticator per message, so compose several restrictions into a single
  `AllOf` authenticator rather than listing them. On Hyperliquid, use an API
  (agent) wallet: it can place and cancel orders but cannot withdraw or
  transfer.
- Use testnet keys for the
  [testnet smoke test](https://daisukeyoda.github.io/godex/guides/smoke-testing/).
  `.env*` and `*.jsonl` are gitignored; never commit key material or account
  recordings.

---

<div align="center">
  <sub>
    <a href="CHANGELOG.md">Changelog</a> ·
    <a href="LICENSE">MIT License</a> ·
    <a href="https://daisukeyoda.github.io/godex/">daisukeyoda.github.io/godex</a>
  </sub>
</div>
