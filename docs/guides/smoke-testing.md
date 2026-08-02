# Testnet Smoke Testing

An adapter is adopted only after it passes the full gate scenario on that
venue's testnet:

connect + verified snapshot → far post-only + cancel → crossing post-only
rejected (normal path) → IOC fill + position → optional forced reconnect
with convergence and duplicate-fill checks → reduce-only close to flat.

This is not a generic integration test — each gate exists to exercise one of
the invariants described in [The Venue Contract](../concepts/contract.md)
against a real venue, not a mock:

- **Connect + verified snapshot** proves the adapter's strict wire validation
  and reconnect/resubscribe path actually converge to a correct account
  state, not just that a socket opens.
- **Far post-only + cancel** proves order placement and cancellation round-trip
  through the venue's real matching and settlement paths.
- **Crossing post-only rejected** proves the *maker orders are post-only*
  invariant end-to-end: a taker-crossing order must come back as
  `AckRejected` on the normal path, never as an error, and never actually
  cross.
- **IOC fill + position** proves fills are observed only from the
  authenticated account stream, and that the resulting position and margin
  state are read correctly from the venue.
- **The optional forced reconnect** proves an adapter recovers into the same
  state after a real disconnect — no missed fills, no duplicated fills, and
  event ordering (`ConnectedEvent`/`DisconnectedEvent` alternation) holds
  across the reconnect.
- **Reduce-only close to flat** proves the adapter can unwind a position
  cleanly, leaving the test account in the same state it started in.

Run the scenario with `cmd/godex-smoke`, one command per venue.

=== "Lighter"

    ```sh
    LIGHTER_ACCOUNT_INDEX=... LIGHTER_API_KEY_INDEX=... LIGHTER_API_PRIVATE_KEY=... \
      go run ./cmd/godex-smoke -venue lighter -network testnet \
      -market-id 2 -symbol SOL-PERP -size 0.200 -reconnect-check \
      [-wait-fill] [-record data/lighter-account.jsonl]
    ```

=== "dYdX"

    ```sh
    DYDX_PRIVATE_KEY_HEX=... DYDX_ADDRESS=dydx1... [DYDX_SUBACCOUNT_NUMBER=0] \
      go run ./cmd/godex-smoke -venue dydx -network testnet \
      -ticker ETH-USD -symbol ETH-PERP -size 0.010 -reconnect-check
    ```

=== "Hyperliquid"

    ```sh
    HYPERLIQUID_ACCOUNT_ADDRESS=0x... HYPERLIQUID_API_PRIVATE_KEY=0x... \
      go run ./cmd/godex-smoke -venue hyperliquid -network testnet \
      -coin ETH -symbol ETH-PERP -size 0.010 -reconnect-check
    ```

!!! note "Logging and exit status"
    Each gate logs `PASS` or `FAIL` as it completes; any failure exits the
    process non-zero, so the run is safe to wire into CI or a pre-release
    checklist without parsing log output.

    `-record` (Lighter only) streams the raw account WS frames to the given
    JSONL path as the scenario runs, for refreshing the adapter's test
    fixtures against real venue traffic.

See [Security](security.md) for the testnet key handling this run assumes —
scoped, trading-only keys, never a withdrawal-capable master key.
