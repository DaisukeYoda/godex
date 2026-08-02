# Security

godex handles authenticated order placement and account-state observation
for real trading accounts. This page collects the security guidance from
the project README in one place, organized by topic.

## Credential handling

Credentials are passed in as struct fields; the library never reads
environment variables or files. `cmd/godex-smoke` reads env vars and fails
fast when they are missing.

## Venue-scoped keys

Use **venue-scoped, trading-only API keys**.

!!! danger "Never expose withdrawal-capable keys to a trading process"
    Withdrawal-capable master keys (L1 wallets) must never reach a trading
    process.

### dYdX: accountplus authenticators

dYdX has no separate API key, so register a dedicated key as a scoped
on-chain authenticator (`accountplus`) and name it in
`Credentials.AuthenticatorID`. The chain takes one authenticator per
message, so compose several restrictions into a single `AllOf`
authenticator rather than listing them.

### Hyperliquid: API (agent) wallets

On Hyperliquid, use an API (agent) wallet: it can place and cancel orders
but cannot withdraw or transfer.

## Testnet keys and secrets

Use testnet keys for the smoke test. `.env*` and `*.jsonl` are gitignored;
never commit key material or account recordings.
