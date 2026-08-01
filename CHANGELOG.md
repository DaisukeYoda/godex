# Changelog

Notable changes per release. Versions follow [semantic versioning](https://semver.org/);
while the major version is 0 the contract may still change between minor
releases, and will until the venue set stops pushing on it.

An adapter is listed as adopted only after the full adoption-gate scenario
passes on testnet — connect and verified snapshot, far post-only and cancel,
crossing post-only rejected on the normal path, IOC fill and position, forced
reconnect with convergence and no duplicate fills, reduce-only close to flat.

## v0.3.0 — Hyperliquid adapter

Adds `hyperliquid`, alongside `lighter` and `dydx`. All three adapters have now
passed their adoption gates on testnet (Hyperliquid on 2026-08-01, ETH).

Also settles what it means for an order to finish: an order's end is reported
exactly once, and a cancel the caller asked for is reported when the venue says
the order ended rather than when it accepts the request. Consumers that counted
`OrderRejectedEvent`s, or that relied on a caller's cancel producing no event,
will see different behaviour — see Fixed.

### Added

- **`hyperliquid` adapter for Hyperliquid, adopted.** The full adoption-gate
  scenario passes on testnet: connect and verified snapshot, far post-only and
  cancel, a crossing post-only refused as `badAloPxRejected` on the normal path,
  IOC fill and long position, a forced reconnect that reconverged on the open
  position with no duplicate fills, and a reduce-only close back to flat.
  Orders are signed as L1 actions — the action is MessagePack-encoded, framed
  with the nonce and vault address, hashed, and signed as EIP-712 typed data
  under the venue's fixed `Exchange` domain — and signing is additionally
  pinned to the reference implementation's own published test
  vectors, including the MessagePack preimage on its own so an encoder change
  reports as an encoding fault rather than an unexplained signature mismatch.
  Post-only maps to the `Alo` time-in-force and a crossing maker is refused
  synchronously, so it returns `AckRejected` in the same call.
- **`godex.VenueHyperliquid`.**
- **Client order ids are minted before dispatch on Hyperliquid**, and are what
  cancels are keyed by. An ambiguous submission therefore still leaves a handle:
  the fault latch reconciles by asking the venue whether it holds that id. A
  definitive "no" untracks the order rather than resending it; a "yes" is
  cancelled, because the caller never received an ack and so has no id to
  address that order with — leaving it resting would be exposure nobody could
  close. An answer the adapter cannot read is treated the same way as no answer
  at all: unknown, and latched. A cancel for an order the venue no longer holds
  reports success, which is what makes a cancel retried after a timeout safe.
- **Hyperliquid reconciles its tracked orders after every reconnect.** Order
  updates are pushed and never replayed, so a cancellation that happened while
  the socket was down would otherwise leave the order tracked forever and its
  `OrderRejectedEvent` never delivered.
- **Hyperliquid maintenance margin follows the venue's tier schedule.** A perp
  advertising 25x drops to 5x above $50k of notional — five times the margin
  requirement. `ExecutionMetadata` carries a single fraction, so the adapter
  publishes the strictest tier: being too conservative costs position size,
  while being too permissive costs the account. The venue omits its flat default
  tables from the response and names them by the leverage they cap at; that
  identity is checked rather than assumed, and a table that cannot be resolved
  fails `Connect` instead of being guessed at.
- **Hyperliquid verifies the account's margin mode before accepting an order.**
  A clearinghouse snapshot omits coins the account is flat in, so a flat
  account's mode is invisible there, and an order action carries no mode of its
  own — an account left on isolated margin would have opened a position the
  adapter's whole-account liquidation math cannot describe.
- **Hyperliquid price quantization follows the venue's two-part rule** — at most
  five significant figures *and* at most `6 - szDecimals` decimals, integers
  always allowed — so the increment is recomputed per order from the price's
  magnitude rather than being a fixed venue tick. Tests assert that rounding a
  price in either direction never produces one the venue would refuse, including
  against live book prices under `GODEX_LIVE_TESTNET`.
- **Opt-in live wire checks for Hyperliquid**
  (`GODEX_LIVE_TESTNET=1 go test ./hyperliquid -run TestLive`). They read public
  endpoints only — no credentials, no order flow — and cover the universe, a
  clearinghouse snapshot, the order-status query the fault latch reconciles
  with, the margin-mode and agent lookups `Connect` performs, whether every
  live perp's margin schedule resolves, and whether the venue still accepts the
  adapter's subscriptions.
- **`github.com/vmihailenco/msgpack/v5`**, the only new module: the venue signs
  over a MessagePack encoding of the action. Keccak and secp256k1 come from
  dependencies godex already had, the latter the same signer the dYdX adapter
  uses.

### Known limitations

- Hyperliquid position and margin are read from clearinghouse snapshots rather
  than pushed: at connect, immediately after every fill, after a reconnect, and
  on a periodic backstop poll. A change this executor did not cause — funding, a
  liquidation, another process on the same account — is therefore visible only
  at the next poll.
- Hyperliquid `Connect` warns, rather than fails, when the signing key is not a
  listed agent of the account. The venue also has a single unnamed agent slot
  that the listing does not cover, so refusing there would reject working
  configurations; a genuinely wrong key is still refused at the first order.
- The single `MaintenanceMarginFraction` describes the strictest tier of a
  Hyperliquid perp's schedule, so risk logic sizing a small position against it
  is more conservative than the venue requires.
- A cancel the caller asks for is not observable on Lighter. `sendTx` accepting
  the transaction is receipt, not application — the venue rejects
  asynchronously — and its account stream reports only the post-only
  cancellation status, with no order-status endpoint to ask instead. So
  cancelling a resting order there produces no `OrderRejectedEvent`, and the
  order stays tracked. Closing this needs the venue's order-status vocabulary,
  which must be established from a recording (`cmd/godex-smoke -record`) rather
  than guessed at: mislabelling a status decides whether a fill is suppressed.
- An unrecognized Hyperliquid order status aborts the connection instead of
  being guessed at. There is no safe default: treating a live order as closed
  makes a strategy requote over its own resting quote, and treating a closed one
  as live leaves it waiting for a fill that will never come. The documented set
  is enumerated in `hyperliquid/constants.go` and asserted complete in the unit
  suite, so a venue that adds a status surfaces as reconnect churn rather than
  as a mis-tracked order.

### Fixed

- **An order's rejection is now reported at most once, on every adapter.** Two
  paths observe the same outcome — the venue's answer to the submission and the
  account stream's own order update — and nothing orders them against each
  other. All three adapters relied on the submission path retiring the order
  before the stream reached it, which held only as long as the stream lagged.
  It does not on Hyperliquid: the testnet adoption run had the `orderUpdates`
  push beat the HTTP response on every crossing post-only, so both paths
  emitted and a caller saw one order rejected twice, under two different
  reason strings. Rejections are now deduplicated by order id at the single
  point every event passes through (`internal/dedupe`), which also absorbs the
  order statuses Lighter's account snapshot replays after a reconnect. A
  strategy that counted rejections, or treated one as a signal to re-quote,
  was acting on the same order twice; one that keyed off the order id was
  already immune.
- **An order that ends by a cancel the caller asked for now reports it, on
  every adapter**, under the new `godex.ReasonCanceledByRequest`. The same
  race ran the other way here: `CancelOrder` untracked on success and the
  account stream's update was only emitted for an order still tracked, so the
  `OrderRejectedEvent` arrived only when the venue's push beat the cancel
  response. Across three otherwise identical testnet runs it appeared twice
  and went missing once. Lighter never emitted one at all, so the three
  adapters did not agree on what a cancel means.

  `CancelOrder` no longer claims an outcome. It records the cancel before
  dispatch and leaves the order tracked; whichever path observes the end
  reports it, and the recorded intent — not which report arrived first — is
  what the reason comes from. Accepting a cancel says the request was valid,
  not that it applied: one accepted in the same instant the order filled
  applied to nothing, and that order is now reported as filled rather than as
  cancelled. A failed cancel withdraws the intent, so the order stays
  addressable and the existing retry-after-timeout recovery is unchanged.
- **Hyperliquid resolves an order the venue says it no longer holds.**
  "never placed, already canceled, or filled" does not say which, and
  untracking on it dropped the `orderUpdates` push that did — an order could
  end with no event at all. The adapter now asks `orderStatus` outright and
  reports what comes back, retiring a filled order without a rejection.
- **Lighter retires an order once it is finished**, instead of remembering
  every order it ever placed for the lifetime of the process. Its cancel of a
  resting order is still not observable at all; the package comment now says
  so rather than implying coverage.
- **dYdX no longer publishes the unpriced position the stream reports after a
  fill.** In that moment the account stream reports the new size with an entry
  price of zero, and an unrealized PnL computed against that zero, correcting
  both in the next update ([#4](https://github.com/DaisukeYoda/godex/issues/4)).
  Size at no price is not a state the venue can be in, so the adapter re-reads
  the REST snapshot instead of emitting it, as it already did for updates that
  omit the priced fields outright. Consumers sizing off position size alone were
  unaffected; one measuring liquidation distance or PnL from entry price could
  briefly read a position the account did not hold.

  REST is served from the same Indexer state, so the re-read can catch the
  transient too: a snapshot still showing it is read up to three times, 150ms
  apart, before being believed. A zero that repeats across those reads is
  published — it is the venue's answer rather than a moment in flight, and
  refusing it indefinitely would strand a position that really is unpriced.
- **A dYdX snapshot re-read can no longer overwrite a newer stream update.** The
  re-read runs on its own goroutine and reserved its observation sequence there,
  so a position update handled by the stream reader in the interim could be
  given the *lower* sequence and then be overwritten by a REST response
  describing an older state — including, when the re-read was prompted by the
  transient above, by the very zero-priced position it went to replace. The
  sequence is now reserved before the goroutine starts.

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
