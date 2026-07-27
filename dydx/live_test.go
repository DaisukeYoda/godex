package dydx

// Opt-in checks against the live testnet. They read public endpoints only — no
// credentials, no order flow — and exist to catch the one class of bug the unit
// suite cannot: a wire shape that differs from what the fixtures assume.
//
//	GODEX_LIVE_TESTNET=1 go test ./dydx -run TestLive -v
//
// A failure here means the venue's payloads have moved and the fixtures are
// stale, which is worth knowing before a smoke run places real orders.

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// liveAddress is a testnet account with trading history. Nothing is signed for
// it; it is read the same way any public explorer would.
const liveAddress = "dydx199tqg4wdlnu4qjlxchpd7seg454937hjrknju4"

func liveContext(t *testing.T) (context.Context, *http.Client, string, string) {
	t.Helper()
	if os.Getenv("GODEX_LIVE_TESTNET") == "" {
		t.Skip("set GODEX_LIVE_TESTNET=1 to run checks against the live testnet")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx, &http.Client{Timeout: restRequestTimeout},
		testnetIndexerRESTBaseURL, testnetRPCBaseURL
}

// TestLiveMarketMetadataParses is the check that matters most for order
// correctness, and it covers every tradable market rather than a chosen one:
// the venue's own numbers must satisfy the identity the adapter asserts when it
// converts a rounded price and size into wire units. If any market's exponents
// disagreed with its tick and step, orders there would be silently mis-sized —
// the remainder-zero assertion in the conversion is the safety net, and this is
// what says the net should never be needed.
func TestLiveMarketMetadataParses(t *testing.T) {
	ctx, client, indexer, _ := liveContext(t)

	response, err := fetchMarkets(ctx, client, indexer)
	if err != nil {
		t.Fatalf("fetchMarkets: %v", err)
	}

	checked, skipped := 0, 0
	for ticker, entry := range *response.Markets {
		// Only markets that accept orders matter; Connect refuses the rest.
		if entry.Status == nil || *entry.Status != marketStatusActive {
			skipped++
			continue
		}
		market, err := response.market(ticker)
		if err != nil {
			t.Errorf("%s: market: %v", ticker, err)
			continue
		}
		meta, err := newMarketMeta(market)
		if err != nil {
			t.Errorf("%s: newMarketMeta: %v", ticker, err)
			continue
		}

		// One tick and one step must land exactly on the venue's integer grid.
		subticks, err := meta.toSubticks(meta.tick)
		if err != nil {
			t.Errorf("%s: one tick does not convert cleanly: %v", ticker, err)
			continue
		}
		quantums, err := meta.toQuantums(meta.step)
		if err != nil {
			t.Errorf("%s: one step does not convert cleanly: %v", ticker, err)
			continue
		}
		if uint64(meta.subticksPerTick) != subticks {
			t.Errorf("%s: one tick is %d subticks, but subticksPerTick is %d",
				ticker, subticks, meta.subticksPerTick)
		}
		if uint64(meta.stepBaseQuantums) != quantums {
			t.Errorf("%s: one step is %d quantums, but stepBaseQuantums is %d",
				ticker, quantums, meta.stepBaseQuantums)
		}
		checked++
	}

	if checked == 0 {
		t.Fatal("no tradable market was checked")
	}
	t.Logf("verified %d tradable markets (%d not tradable)", checked, skipped)
}

// TestLiveIndexerHeightNeverLeadsTheChain pins the reason good_til_block is
// derived from the validator rather than the Indexer. The Indexer trails the
// chain, so an order priced off its height would be given a shorter life than
// intended — or, past the window, none at all.
//
// The Indexer is read first so the comparison stays sound: the chain can only
// have advanced in between, never gone backwards.
func TestLiveIndexerHeightNeverLeadsTheChain(t *testing.T) {
	ctx, client, indexer, rpc := liveContext(t)

	response, err := getJSON[indexerHeightResponse](ctx, client, indexer, "/height")
	if err != nil {
		t.Fatalf("indexer height: %v", err)
	}
	indexerHeight, err := strconv.ParseUint(*response.Height, 10, 32)
	if err != nil {
		t.Fatalf("indexer height %q: %v", *response.Height, err)
	}
	chainHeight, err := fetchHeight(ctx, client, rpc)
	if err != nil {
		t.Fatalf("fetchHeight: %v", err)
	}

	t.Logf("indexer height %d, chain height %d (lag %d blocks)",
		indexerHeight, chainHeight, int64(chainHeight)-int64(indexerHeight))
	if chainHeight == 0 {
		t.Fatal("chain height must be non-zero")
	}
	if uint64(chainHeight) < indexerHeight {
		t.Fatalf("the Indexer led the chain (%d > %d), which the height choice assumes cannot happen",
			indexerHeight, chainHeight)
	}
}

// indexerHeightResponse exists only for the comparison above; the adapter never
// uses the Indexer's height.
type indexerHeightResponse struct {
	Height *string `json:"height"`
}

func (r *indexerHeightResponse) validate() error {
	if r.Height == nil {
		return missingField("indexer height", "height")
	}
	return nil
}

// TestLiveAccountQuery exercises the abci_query path that replaces a Cosmos LCD
// host — the part of the transaction envelope with no reference implementation
// in the sources this adapter was written from.
func TestLiveAccountQuery(t *testing.T) {
	ctx, client, _, rpc := liveContext(t)

	accountNumber, sequence, err := fetchAccountInfo(ctx, client, rpc, liveAddress)
	if err != nil {
		t.Fatalf("fetchAccountInfo: %v", err)
	}
	t.Logf("account number %d, sequence %d", accountNumber, sequence)
	if accountNumber == 0 && sequence == 0 {
		t.Fatal("an account with history should report a non-zero account number or sequence")
	}
}

// TestLiveSubaccountAndHistoryDecode runs the strict decoders over real
// payloads. Fills in particular are returned across every market the subaccount
// has traded, which is why the executor filters them.
func TestLiveSubaccountAndHistoryDecode(t *testing.T) {
	ctx, client, indexer, _ := liveContext(t)

	account, err := fetchSubaccount(ctx, client, indexer, liveAddress, 0)
	if err != nil {
		t.Fatalf("fetchSubaccount: %v", err)
	}
	t.Logf("equity=%s freeCollateral=%s openPositions=%d",
		*account.Subaccount.Equity, *account.Subaccount.FreeCollateral,
		len(account.Subaccount.OpenPerpetualPositions))

	fills, err := fetchFills(ctx, client, indexer, liveAddress, 0, fillBackfillLimit, "")
	if err != nil {
		t.Fatalf("fetchFills: %v", err)
	}
	markets, unattributed := map[string]int{}, 0
	for _, entry := range *fills.Fills {
		markets[entry.marketTicker()]++
		if entry.OrderID == nil {
			unattributed++
		}
		if _, err := time.Parse(time.RFC3339, *entry.CreatedAt); err != nil {
			t.Fatalf("fill createdAt %q does not parse: %v", *entry.CreatedAt, err)
		}
	}
	t.Logf("fills=%d across %d markets, %d without an orderId",
		len(*fills.Fills), len(markets), unattributed)

	orders, err := fetchOpenOrders(ctx, client, indexer, liveAddress, 0)
	if err != nil {
		t.Fatalf("fetchOpenOrders: %v", err)
	}
	t.Logf("open orders=%d", len(orders.Orders))
	for i := range orders.Orders {
		if _, err := clientIDOf(&orders.Orders[i]); err != nil {
			t.Fatalf("open order clientId: %v", err)
		}
	}
}

// TestLiveAccountStreamDecodes subscribes to the public subaccounts channel and
// runs every frame it receives through the executor's own decoder, so an
// unexpected message type or payload shape shows up here rather than as an
// aborted connection during a smoke run — which is exactly how the streamed
// fill's market field was found to differ from the REST one.
//
// What it can guarantee depends on the account: the subscription snapshot
// always arrives, so that shape is always covered. Incremental frames only
// arrive if something happens to the account while the test is watching, so
// they are decoded when present and reported when not, rather than being
// claimed as verified. The smoke run covers them directly.
func TestLiveAccountStreamDecodes(t *testing.T) {
	ctx, _, _, _ := liveContext(t)

	conn, response, err := websocket.DefaultDialer.DialContext(ctx, testnetIndexerWSURL, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial %s: %v", testnetIndexerWSURL, err)
	}
	defer func() { _ = conn.Close() }()

	subscribe := `{"type":"subscribe","channel":"` + subaccountsChannel +
		`","id":"` + liveAddress + `/0"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(subscribe)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Keep reading past the snapshot: an idle account produces nothing more,
	// but a busy one exercises the incremental shapes for free.
	deadline := time.Now().Add(liveStreamWindow)
	_ = conn.SetReadDeadline(deadline)
	seen := map[string]int{}
	fills, orders := 0, 0
	for time.Now().Before(deadline) {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break // the read deadline closing the window, not a failure
		}
		message, err := decodeSubaccountWsMessage(raw)
		if err != nil {
			t.Fatalf("the adapter cannot decode a live frame: %v\nframe: %s", err, raw)
		}
		seen[message.Type]++
		if message.Contents == nil {
			continue
		}
		for i := range message.Contents.Fills {
			fills++
			// Whichever field the stream uses, the market must resolve, or the
			// executor cannot tell its own executions from another market's.
			if message.Contents.Fills[i].marketTicker() == "" {
				t.Fatalf("a streamed fill named no market:\n%s", raw)
			}
		}
		orders += len(message.Contents.Orders)

		if message.Type == wsTypeSubscribed {
			if message.Contents.Subaccount == nil {
				t.Fatal("the subscription snapshot carried no subaccount state")
			}
			if _, err := normalizeSnapshot(message.Contents.Subaccount,
				normalizeContext{symbol: "ETH-PERP", ticker: "ETH-USD", receivedAt: time.Now()}); err != nil {
				t.Fatalf("normalizeSnapshot on live data: %v", err)
			}
			t.Logf("snapshot equity=%s positions=%d orders=%d",
				*message.Contents.Subaccount.Equity,
				len(message.Contents.Subaccount.OpenPerpetualPositions),
				len(message.Contents.Orders))
		}
	}

	if seen[wsTypeSubscribed] == 0 {
		t.Fatal("no subscription snapshot arrived")
	}
	t.Logf("frames by type: %v (fills=%d, order updates=%d)", seen, fills, orders)
	if seen[wsTypeChannelData] == 0 {
		t.Logf("note: the account was idle, so no incremental frame was exercised; " +
			"the smoke run covers those")
	}
}

// liveStreamWindow is how long the stream check watches for incremental frames
// after the snapshot. Long enough to catch activity on a busy account, short
// enough not to stall an opt-in check on an idle one.
const liveStreamWindow = 10 * time.Second
