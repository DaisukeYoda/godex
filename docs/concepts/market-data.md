# Market Data

Alongside execution, godex normalizes the public market data a maker/taker
strategy consumes — order books and funding — behind two contracts, one
streamed and one polled. These are public, read-only, and need no
credentials. Both are available for Lighter and dYdX today.

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

## Invariants

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

## dYdX extras

dYdX also exposes `dydx.FetchFundingPayments` (the Indexer's per-account
settled funding history, public REST) and `dydx.KeyFromMnemonic`, which
derives the account key at the Cosmos HD path `m/44'/118'/0'/0/0` exactly as
the official clients do — pinned in tests against `@dydxprotocol/v4-client-js`.

## Watching live market data

To watch live market data without touching execution:

```sh
go run ./cmd/godex-smoke -market-watch -venue dydx -network mainnet \
  -ticker SOL-USD -symbol SOL-PERP -price-scale 4 -size-scale 3 -watch-duration 30s
```
