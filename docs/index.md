# godex

Go trading integration layer for perpetual DEXes — Lighter (zkLighter), dYdX
v4, and Hyperliquid. godex owns authenticated order placement, cancellation,
account-state observation, and venue-specific signing behind a small,
safety-oriented contract. Strategy and risk logic depend only on the
normalized types and events; venue protocol details never leak out. This is
**not** a generic exchange SDK — the contract intentionally supports exactly
what a post-only maker / IOC taker strategy needs.

**Status: pre-release.** All three adapters are implemented with full unit
suites, and all three have passed the full testnet adoption-gate run.

## Install

```sh
go get github.com/DaisukeYoda/godex
```

## Where to go next

- [Getting Started](getting-started.md) — install and run a first order
- [The Venue Contract](concepts/contract.md) — the `VenueExecutor` interface and its invariants
- [Market Data](concepts/market-data.md) — order-book streams and funding rates
- [Testnet Smoke Testing](guides/smoke-testing.md) — the adoption-gate run
- [Security](guides/security.md) — credential handling and key scoping
- [Design Notes](design/pre-implementation.md) — the design boundary and safety rules
