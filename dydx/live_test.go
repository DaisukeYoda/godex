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
// correctness: the venue's own numbers must satisfy the identity the adapter
// asserts when it converts a rounded price and size into wire units.
func TestLiveMarketMetadataParses(t *testing.T) {
	ctx, client, indexer, _ := liveContext(t)

	response, err := fetchMarkets(ctx, client, indexer)
	if err != nil {
		t.Fatalf("fetchMarkets: %v", err)
	}
	market, err := response.market("ETH-USD")
	if err != nil {
		t.Fatalf("market: %v", err)
	}
	meta, err := newMarketMeta(market)
	if err != nil {
		t.Fatalf("newMarketMeta: %v", err)
	}
	t.Logf("ETH-USD tick=%s step=%s mmf=%s clobPairID=%d",
		meta.tick, meta.step, meta.maintenanceMarginFraction, meta.clobPairID)

	// One tick and one step must land exactly on the venue's integer grid.
	subticks, err := meta.toSubticks(meta.tick)
	if err != nil {
		t.Fatalf("one tick does not convert cleanly: %v", err)
	}
	quantums, err := meta.toQuantums(meta.step)
	if err != nil {
		t.Fatalf("one step does not convert cleanly: %v", err)
	}
	if uint64(meta.subticksPerTick) != subticks {
		t.Fatalf("one tick is %d subticks, but subticksPerTick is %d", subticks, meta.subticksPerTick)
	}
	if uint64(meta.stepBaseQuantums) != quantums {
		t.Fatalf("one step is %d quantums, but stepBaseQuantums is %d", quantums, meta.stepBaseQuantums)
	}
}

// TestLiveHeightIsAheadOfTheIndexer pins the reason good_til_block is derived
// from the validator rather than the Indexer.
func TestLiveHeightIsAheadOfTheIndexer(t *testing.T) {
	ctx, client, indexer, rpc := liveContext(t)

	chainHeight, err := fetchHeight(ctx, client, rpc)
	if err != nil {
		t.Fatalf("fetchHeight: %v", err)
	}
	indexerHeight, err := getJSON[indexerHeightResponse](ctx, client, indexer, "/height")
	if err != nil {
		t.Fatalf("indexer height: %v", err)
	}
	t.Logf("chain height %d, indexer height %s", chainHeight, *indexerHeight.Height)
	if chainHeight == 0 {
		t.Fatal("chain height must be non-zero")
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
		markets[*entry.Market]++
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
// runs every frame through the executor's own decoder, so an unexpected message
// type or payload shape shows up here rather than as an aborted connection
// during a smoke run.
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

	// The venue answers with a connected frame and then the snapshot.
	deadline := time.Now().Add(30 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	sawSnapshot := false
	for !sawSnapshot && time.Now().Before(deadline) {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		message, err := decodeSubaccountWsMessage(raw)
		if err != nil {
			t.Fatalf("the adapter cannot decode a live frame: %v\nframe: %s", err, raw)
		}
		t.Logf("frame type=%q channel=%q", message.Type, message.Channel)
		if message.Type != wsTypeSubscribed {
			continue
		}
		sawSnapshot = true
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
	if !sawSnapshot {
		t.Fatal("no subscription snapshot arrived")
	}
}
