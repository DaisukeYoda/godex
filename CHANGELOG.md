# Changelog

Notable changes per release. Versions follow [semantic versioning](https://semver.org/);
while the major version is 0 the contract may still change between minor
releases, and will until the venue set stops pushing on it.

An adapter is listed as adopted only after the full adoption-gate scenario
passes on testnet — connect and verified snapshot, far post-only and cancel,
crossing post-only rejected on the normal path, IOC fill and position, forced
reconnect with convergence and no duplicate fills, reduce-only close to flat.

## v0.2.0 — dYdX v4 adapter

Adds `dydx`, alongside the existing `lighter`. Both adapters have now passed
their adoption gates on testnet (dYdX on 2026-07-27, ETH-USD).

### Added

- **`dydx` adapter for dYdX v4.** Short-term orders: gas-free, matched
  synchronously in CheckTx — so a crossing post-only returns `AckRejected` in
  the same call — and valid for at most ~20 blocks. Transactions are built and
  signed in process (SIGN_MODE_DIRECT secp256k1) from a vendored protobuf
  subset, rather than importing the dYdX chain module, whose forked cosmos-sdk
  pins do not resolve transitively. Talks to exactly two hosts: the Indexer for
  market and account state, a validator's CometBFT RPC for block height,
  account lookups, and broadcast.
- **`godex.VenueDydx`.**
- **Opt-in live wire checks** (`GODEX_LIVE_TESTNET=1 go test ./dydx -run TestLive`).
  They read public testnet endpoints — no credentials, no order flow — and push
  real responses through the adapter's strict decoders, catching the one class
  of bug fixtures cannot: a venue payload that has moved. This is how the
  streamed fill's market field was found to differ from the REST one.
- **`smoketest.Collector.WaitForAt`**, which reports where a match landed so an
  ordered sequence can continue strictly after it.

### Changed

- **`OrderRejectedEvent` documents what it always meant.** It closes an order
  rather than voiding it: an IOC that fills part of its size and has the
  remainder cancelled produces both a `FillEvent` and this, and the rejection
  can arrive *before* the fill it accounts for, because the venue reports the
  removal in an earlier message. Behaviour is unchanged; the previous wording
  invited a reading under which a consumer would mis-book the executed part.
- **The smoke harness checks the reconnect sequence in order.** Every wait
  previously started from the same mark, so an update still in flight when the
  socket dropped could satisfy "post-reconnect position snapshot" — letting a
  broken snapshot pass, and letting an event landing between the baseline and
  the mark fail a healthy executor. Applies to every venue, Lighter included.
- **`VenueExecutor` states that `Connect` must return before any other call.**
  Nothing else establishes that ordering; it was previously only implied.

### Known limitations

- **Testnet only.** Neither adapter has been run against mainnet.
- dYdX position updates can pass through an intermediate carrying a zero entry
  price before the correct one arrives ([#4](https://github.com/DaisukeYoda/godex/issues/4)).
- dYdX scoped-key signing (`accountplus` authenticators) is implemented but has
  not been exercised on chain; the testnet run signed as the account owner.
- No Hyperliquid adapter yet.

## v0.1.0 — Lighter adapter

First release: the normalized `VenueExecutor` contract and the `lighter`
(zkLighter) adapter, with the venue-agnostic adoption-gate smoke harness.
