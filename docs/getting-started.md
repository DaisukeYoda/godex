# Getting Started

## Install

```sh
go get github.com/DaisukeYoda/godex
```

## Quickstart

Each adapter is constructed with a venue-specific `Config` and then satisfies
the same `godex.VenueExecutor` contract — `Connect`, `PlaceOrder`,
`CancelOrder`, `AccountEvents`, `Close`. Pick your venue below.

=== "Lighter"

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

=== "dYdX v4"

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
    synchronously in CheckTx (so a crossing post-only comes back as
    `AckRejected` in the same call), and valid for at most ~20 blocks. It
    talks to exactly two hosts: the Indexer (market metadata, account stream)
    and a validator's CometBFT RPC (block height, account lookup, broadcast).
    Transactions are built and signed in-process from a small vendored
    protobuf set (`dydx/internal/pb`) rather than importing the dYdX chain
    module, whose forked cosmos-sdk pins do not resolve transitively.

=== "Hyperliquid"

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

    Orders are signed as L1 actions: the action is MessagePack-encoded,
    framed with the nonce and vault address, hashed, and signed as EIP-712
    typed data under the venue's fixed `Exchange` domain. Signing is
    implemented in-process and pinned to the reference implementation's
    published test vectors (`hyperliquid/signer_test.go`); only MessagePack
    comes from a third-party module, with keccak and secp256k1 taken from
    dependencies godex already uses.

    Post-only maps to the venue's `Alo` time-in-force, and a crossing maker
    is refused synchronously, so it returns `AckRejected` in the same call.
    Every order is submitted under a client order id minted before dispatch,
    which is also what a cancel is keyed by and what an ambiguous submission
    is reconciled against — an order the venue turns out to be holding is
    cancelled, since the caller never received an id it could close it with.
    Fills come from the `userFills` stream; position and margin are read
    from clearinghouse snapshots — at connect, on every fill, after a
    reconnect, and on a periodic backstop poll. Tracked orders are
    re-checked after a reconnect too, because order updates are pushed and
    never replayed.

    Maintenance margin on Hyperliquid is tiered — a perp advertising 25x
    drops to 5x above $50k of notional — so `MaintenanceMarginFraction`,
    which is a single ratio, carries the strictest tier rather than the
    headline one.

For the full set of invariants every adapter upholds — post-only semantics,
fill sourcing, event ordering, and more — see
[the contract](concepts/contract.md). Before building on any of this,
validate your setup against testnet with the
[smoke test guide](guides/smoke-testing.md).
