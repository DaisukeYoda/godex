# godex

Go trading integration layer for perpetual DEXes — Lighter (zkLighter) today,
dYdX v4 and Hyperliquid planned. godex owns authenticated order placement,
cancellation, account-state observation, and venue-specific signing behind a
small, safety-oriented contract. Strategy and risk logic depend only on the
normalized types and events; venue protocol details never leak out.

This is **not** a generic exchange SDK. The contract intentionally supports
exactly what a post-only maker / IOC taker strategy needs. See
`docs/pre-implementation.md` for the design boundary and safety rules.

**Status: pre-release.** The Lighter adapter is implemented with a full unit
suite; final adoption sign-off requires the testnet smoke run below.

## Install

```sh
go get github.com/DaisukeYoda/godex
```

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
- **Money is fixed-point.** All prices/sizes use `decimal.Decimal` (big-int
  mantissa + scale, string-only construction, round half away from zero).
  No floats anywhere near order flow.

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

Each gate logs PASS/FAIL; any failure exits non-zero. `-record` streams raw
account WS frames to JSONL for fixture refresh.

## Security

- Credentials are passed in as struct fields; the library never reads
  environment variables or files. `cmd/godex-smoke` reads env vars and fails
  fast when they are missing.
- Use **venue-scoped, trading-only API keys**. Withdrawal-capable master keys
  (L1 wallets) must never reach a trading process.
- Use testnet keys for the smoke test. `.env*` and `*.jsonl` are gitignored;
  never commit key material or account recordings.

## Roadmap

- `dydx` adapter (short-term orders, permissioned authenticator, goodTilBlock)
- `hyperliquid` adapter (EIP-712 actions, agent wallets)

## License

MIT
