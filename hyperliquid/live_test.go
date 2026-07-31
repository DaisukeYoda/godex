package hyperliquid

// Opt-in checks against the live testnet. They read public endpoints only — no
// credentials, no order flow — and exist to catch the one class of bug the unit
// suite cannot: a wire shape that differs from what the fixtures assume.
//
//	GODEX_LIVE_TESTNET=1 go test ./hyperliquid -run TestLive -v
//
// A failure here means the venue's payloads have moved and the fixtures are
// stale, which is worth knowing before a smoke run places real orders.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/decimal"
	"github.com/gorilla/websocket"
)

// liveAddress is read the same way any public explorer would read it. The zero
// address always answers with a well-formed, empty account, so the shape can be
// checked without depending on anyone's balance.
const liveAddress = "0x0000000000000000000000000000000000000000"

func liveContext(t *testing.T) (context.Context, *http.Client) {
	t.Helper()
	if os.Getenv("GODEX_LIVE_TESTNET") == "" {
		t.Skip("set GODEX_LIVE_TESTNET=1 to run checks against the live testnet")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx, &http.Client{Timeout: restRequestTimeout}
}

// TestLiveMetaParses is the check that matters most for order correctness: the
// asset id every order is keyed by is a position in this array, and the
// quantization rules are derived from its entries.
func TestLiveMetaParses(t *testing.T) {
	ctx, client := liveContext(t)

	response, err := postJSON[metaResponse](ctx, client, testnetRESTBaseURL, infoRequest{Type: "meta"})
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	if len(*response.Universe) == 0 {
		t.Fatal("meta returned an empty universe")
	}

	checked := 0
	for i := range *response.Universe {
		entry := &(*response.Universe)[i]
		if entry.IsDelisted != nil && *entry.IsDelisted {
			continue
		}
		if _, err := maintenanceMarginFraction(*entry.MaxLeverage); err != nil {
			t.Errorf("%s: maintenanceMarginFraction: %v", *entry.Name, err)
			continue
		}
		// Every magnitude an order could be priced at must quantize to
		// something the venue accepts.
		for _, price := range []string{"0.00012345", "0.4321", "9.87654", "1234.5678", "98765.4"} {
			tick, err := priceTick(mustDecimal(t, price), *entry.SzDecimals)
			if err != nil {
				t.Errorf("%s: priceTick(%s): %v", *entry.Name, price, err)
				continue
			}
			for _, side := range []godex.Side{godex.SideBuy, godex.SideSell} {
				rounded, err := godex.RoundPriceToTick(mustDecimal(t, price), tick, side)
				if err != nil {
					t.Errorf("%s: RoundPriceToTick(%s): %v", *entry.Name, price, err)
					continue
				}
				assertVenuePrice(t, wireDecimal(rounded), *entry.SzDecimals)
			}
		}
		checked++
	}
	t.Logf("checked %d live perps", checked)
}

func TestLiveClearinghouseStateParses(t *testing.T) {
	ctx, client := liveContext(t)

	state, err := postJSON[clearinghouseState](ctx, client, testnetRESTBaseURL,
		infoRequest{Type: "clearinghouseState", User: liveAddress})
	if err != nil {
		t.Fatalf("clearinghouseState: %v", err)
	}
	snapshot, err := normalizeAccount(state, normalizeContext{
		symbol: testSymbol, coin: testCoin, receivedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("normalizeAccount: %v", err)
	}
	t.Logf("live snapshot: position=%s equity=%s usage=%s",
		snapshot.position.Size, snapshot.margin.EquityUSD, snapshot.margin.UsageRatio)
}

// TestLiveOrderStatusParses covers the query the adapter reconciles an
// ambiguous submission with. A client order id that was never used must come
// back as a definitive "unknownOid".
func TestLiveOrderStatusParses(t *testing.T) {
	ctx, client := liveContext(t)

	response, err := postJSON[orderQueryResponse](ctx, client, testnetRESTBaseURL, infoRequest{
		Type: "orderStatus",
		User: liveAddress,
		Oid:  "0x00000000000000000000000000000001",
	})
	if err != nil {
		t.Fatalf("orderStatus: %v", err)
	}
	if *response.Status != queryStatusUnknownOid {
		t.Errorf("status = %q, want %q for an unused client order id", *response.Status, queryStatusUnknownOid)
	}
}

// TestLiveSubscriptionsAreAccepted proves the channel names and the subscribe
// envelope still match: a renamed channel comes back as an error notice rather
// than as a subscription response.
func TestLiveSubscriptionsAreAccepted(t *testing.T) {
	ctx, _ := liveContext(t)

	conn, response, err := websocket.DefaultDialer.DialContext(ctx, testnetWSURL, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, channel := range []string{channelUserFills, channelOrderUpdates} {
		message, err := json.Marshal(map[string]any{
			"method":       "subscribe",
			"subscription": map[string]string{"type": channel, "user": liveAddress},
		})
		if err != nil {
			t.Fatalf("encode subscribe: %v", err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
			t.Fatalf("subscribe %s: %v", channel, err)
		}
	}

	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	accepted := 0
	for accepted < 2 {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v (accepted %d of 2 subscriptions)", err, accepted)
		}
		var envelope wsEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		if err := envelope.validate(); err != nil {
			t.Fatalf("validate envelope: %v", err)
		}
		switch *envelope.Channel {
		case channelSubscriptionResponse:
			accepted++
		case channelError:
			t.Fatalf("the venue refused a subscription: %s", string(envelope.Data))
		case channelUserFills:
			// The snapshot may arrive before the second acknowledgement.
			if err := decodeAndValidate[wsUserFills](t, string(envelope.Data)); err != nil {
				t.Fatalf("userFills snapshot does not validate: %v", err)
			}
		case channelOrderUpdates:
			var updates []wsOrderUpdate
			if err := json.Unmarshal(envelope.Data, &updates); err != nil {
				t.Fatalf("decode orderUpdates: %v", err)
			}
			for i := range updates {
				if err := updates[i].validate(); err != nil {
					t.Fatalf("orderUpdates entry does not validate: %v", err)
				}
			}
		default:
			t.Fatalf("unexpected channel %q", *envelope.Channel)
		}
	}
}

// TestLiveL2BookPricesQuantize pushes real prices through the rounding rule,
// which synthetic magnitudes cannot: a live book says what the venue actually
// quotes.
func TestLiveL2BookPricesQuantize(t *testing.T) {
	ctx, client := liveContext(t)

	meta, err := postJSON[metaResponse](ctx, client, testnetRESTBaseURL, infoRequest{Type: "meta"})
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	var szDecimals int
	found := false
	for i := range *meta.Universe {
		if *(*meta.Universe)[i].Name == testCoin {
			szDecimals, found = *(*meta.Universe)[i].SzDecimals, true
			break
		}
	}
	if !found {
		t.Fatalf("%s is not in the live universe", testCoin)
	}

	body, err := doPost(ctx, client, testnetRESTBaseURL+infoPath,
		map[string]string{"type": "l2Book", "coin": testCoin})
	if err != nil {
		t.Fatalf("l2Book: %v", err)
	}
	var book struct {
		Levels [][]struct {
			Px *string `json:"px"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(body, &book); err != nil {
		t.Fatalf("l2Book returned malformed JSON: %v", err)
	}
	if len(book.Levels) != 2 || len(book.Levels[0]) == 0 || len(book.Levels[1]) == 0 {
		t.Fatalf("l2Book returned an unusable book: %s", truncate(body))
	}

	for _, side := range book.Levels {
		for _, level := range side {
			if level.Px == nil {
				t.Fatal("l2Book level is missing px")
			}
			price, err := decimal.FromDecimalString(*level.Px)
			if err != nil {
				t.Fatalf("l2Book price %q: %v", *level.Px, err)
			}
			// A price the venue itself quotes must survive quantization
			// unchanged, in both directions.
			tick, err := priceTick(price, szDecimals)
			if err != nil {
				t.Fatalf("priceTick(%s): %v", price, err)
			}
			for _, orderSide := range []godex.Side{godex.SideBuy, godex.SideSell} {
				rounded, err := godex.RoundPriceToTick(price, tick, orderSide)
				if err != nil {
					t.Fatalf("RoundPriceToTick(%s): %v", price, err)
				}
				if wireDecimal(rounded) != wireDecimal(price) {
					t.Errorf("live %s price %s does not survive rounding (%s): got %s",
						testCoin, price, orderSide, wireDecimal(rounded))
				}
			}
		}
	}
}

// TestLiveMarginTablesResolve is the check behind the maintenance-margin
// number the adapter publishes. The venue omits its flat default tables from
// the response and names them by the leverage they cap at; that identity is
// what makes a lookup miss safe to read, so it is asserted over the whole live
// universe rather than assumed.
func TestLiveMarginTablesResolve(t *testing.T) {
	ctx, client := liveContext(t)

	response, err := postJSON[metaResponse](ctx, client, testnetRESTBaseURL, infoRequest{Type: infoTypeMeta})
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	tables := response.marginTablesByID()

	tiered, flat := 0, 0
	for i := range *response.Universe {
		entry := &(*response.Universe)[i]
		leverage, err := maintenanceLeverage(entry, tables)
		if err != nil {
			t.Errorf("%s: %v", *entry.Name, err)
			continue
		}
		if leverage > *entry.MaxLeverage {
			t.Errorf("%s: maintenance leverage %d exceeds max leverage %d",
				*entry.Name, leverage, *entry.MaxLeverage)
		}
		if _, resolved := tables[*entry.MarginTableID]; resolved {
			tiered++
		} else {
			flat++
		}
	}
	t.Logf("resolved %d tiered and %d flat-default margin schedules", tiered, flat)
}

// TestLiveActiveAssetDataParses covers the margin-mode check Connect runs
// before accepting any order.
func TestLiveActiveAssetDataParses(t *testing.T) {
	ctx, client := liveContext(t)

	response, err := postJSON[activeAssetDataResponse](ctx, client, testnetRESTBaseURL,
		infoRequest{Type: infoTypeActiveAssetData, User: liveAddress, Coin: testCoin})
	if err != nil {
		t.Fatalf("activeAssetData: %v", err)
	}
	t.Logf("live %s margin mode: %s", *response.Coin, *response.Leverage.Type)
}

// TestLiveExtraAgentsParses covers the credential check Connect warns from.
func TestLiveExtraAgentsParses(t *testing.T) {
	ctx, client := liveContext(t)

	agents, err := postJSON[extraAgentList](ctx, client, testnetRESTBaseURL,
		infoRequest{Type: infoTypeExtraAgents, User: liveAddress})
	if err != nil {
		t.Fatalf("extraAgents: %v", err)
	}
	t.Logf("live account lists %d agents", len(*agents))
}
