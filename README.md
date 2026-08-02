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
suites, and all three have passed the full testnet adoption-gate run.

| Venue | Execution | Market data | Adoption gate |
| :--- | :---: | :---: | :---: |
| **Lighter** (zkLighter) | ✅ | ✅ | ✅ passed |
| **dYdX v4** | ✅ | ✅ | ✅ passed |
| **Hyperliquid** | ✅ | — | ✅ passed |

## Install

```sh
go get github.com/DaisukeYoda/godex
```

## Quickstart (Lighter, testnet)

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/DaisukeYoda/godex"
    "github.com/DaisukeYoda/godex/decimal"
    "github.com/DaisukeYoda/godex/lighter"
)

func main() {
    exec, err := lighter.New(lighter.Config{
        Credentials: lighter.Credentials{
            AccountIndex:  48,
            APIKeyIndex:   2,
            APIPrivateKey: os.Getenv("LIGHTER_API_PRIVATE_KEY"),
        },
        Symbol:   "SOL-PERP",
        MarketID: 2, // SOL on testnet
        Network:  lighter.Testnet,
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
        Symbol: "SOL-PERP",
        Side:   godex.SideBuy,
        Price:  decimal.MustFromString("80.000", 3), // rounded to tick by the adapter
        Size:   decimal.MustFromString("0.200", 3),
        Intent: godex.IntentPostOnly,
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("ack: %+v", ack)
}
```

## Quickstart (dYdX v4, testnet)

```go
exec, err := dydx.New(dydx.Config{
    Credentials: dydx.Credentials{
        PrivateKeyHex: os.Getenv("DYDX_PRIVATE_KEY_HEX"),
        Address:       os.Getenv("DYDX_ADDRESS"), // dydx1... — the account orders belong to
        SubaccountNumber: 0,
        // Optional: the on-chain authenticator scoping this key to trading only.
        AuthenticatorID: nil,
    },
    Symbol:  "ETH-PERP",
    Ticker:  "ETH-USD", // venue market ticker
    Network: dydx.Testnet,
})
```

The dYdX adapter places **short-term orders only** — gas-free, matched
synchronously in CheckTx (so a crossing post-only comes back as `AckRejected`
in the same call), and valid for at most ~20 blocks. It talks to exactly two
hosts: the Indexer (market metadata, account stream) and a validator's CometBFT
RPC (block height, account lookup, broadcast). Transactions are built and signed
in-process from a small vendored protobuf set (`dydx/internal/pb`) rather than
importing the dYdX chain module, whose forked cosmos-sdk pins do not resolve
transitively.

## Quickstart (Hyperliquid, testnet)

```go
exec, err := hyperliquid.New(hyperliquid.Config{
    Credentials: hyperliquid.Credentials{
        // The account that holds the position — the master account when an
        // API wallet signs for it.
        AccountAddress: os.Getenv("HYPERLIQUID_ACCOUNT_ADDRESS"), // 0x...
        APIPrivateKey:  os.Getenv("HYPERLIQUID_API_PRIVATE_KEY"), // API (agent) wallet key
        // Optional: route orders to a vault or subaccount instead.
        VaultAddress: "",
    },
    Symbol:  "ETH-PERP",
    Coin:    "ETH", // venue perp name
    Network: hyperliquid.Testnet,
})
```

Orders are signed as L1 actions: the action is MessagePack-encoded, framed with
the nonce and vault address, hashed, and signed as EIP-712 typed data under the
venue's fixed `Exchange` domain. Signing is implemented in-process and pinned to
the reference implementation's published test vectors
(`hyperliquid/signer_test.go`); only MessagePack comes from a third-party
module, with keccak and secp256k1 taken from dependencies godex already uses.

Post-only maps to the venue's `Alo` time-in-force, and a crossing maker is
refused synchronously, so it returns `AckRejected` in the same call. Every order
is submitted under a client order id minted before dispatch, which is also what
a cancel is keyed by and what an ambiguous submission is reconciled against — an
order the venue turns out to be holding is cancelled, since the caller never
received an id it could close it with. Fills come from the `userFills` stream;
position and margin are read from clearinghouse snapshots — at connect, on every
fill, after a reconnect, and on a periodic backstop poll. Tracked orders are
re-checked after a reconnect too, because order updates are pushed and never
replayed.

Maintenance margin on Hyperliquid is tiered — a perp advertising 25x drops to 5x
above $50k of notional — so `MaintenanceMarginFraction`, which is a single
ratio, carries the strictest tier rather than the headline one.

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

## Testnet smoke test (adoption gates)

An adapter is adopted only after the full gate scenario passes on testnet:
connect + verified snapshot → far post-only + cancel → crossing post-only
rejected (normal path) → IOC fill + position → optional forced reconnect with
convergence and duplicate-fill checks → reduce-only close to flat.

```sh
LIGHTER_ACCOUNT_INDEX=... LIGHTER_API_KEY_INDEX=... LIGHTER_API_PRIVATE_KEY=... \
  go run ./cmd/godex-smoke -venue lighter -network testnet \
  -market-id 2 -symbol SOL-PERP -size 0.200 -reconnect-check \
  [-wait-fill] [-record data/lighter-account.jsonl]
```

```sh
DYDX_PRIVATE_KEY_HEX=... DYDX_ADDRESS=dydx1... [DYDX_SUBACCOUNT_NUMBER=0] \
  go run ./cmd/godex-smoke -venue dydx -network testnet \
  -ticker ETH-USD -symbol ETH-PERP -size 0.010 -reconnect-check
```

```sh
HYPERLIQUID_ACCOUNT_ADDRESS=0x... HYPERLIQUID_API_PRIVATE_KEY=0x... \
  go run ./cmd/godex-smoke -venue hyperliquid -network testnet \
  -coin ETH -symbol ETH-PERP -size 0.010 -reconnect-check
```

Each gate logs PASS/FAIL; any failure exits non-zero. `-record` streams raw
account WS frames to JSONL for fixture refresh (Lighter only).

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
- Use testnet keys for the smoke test. `.env*` and `*.jsonl` are gitignored;
  never commit key material or account recordings.

---

<div align="center">
  <sub>
    <a href="CHANGELOG.md">Changelog</a> ·
    <a href="LICENSE">MIT License</a> ·
    <a href="https://daisukeyoda.github.io/godex/">daisukeyoda.github.io/godex</a>
  </sub>
</div>
