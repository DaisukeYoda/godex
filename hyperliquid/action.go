package hyperliquid

// Exchange actions and their signing preimage.
//
// An action is submitted as JSON but signed over its MessagePack encoding, so
// the venue re-encodes the parsed action and compares. Field order, key
// spelling, and integer width are therefore part of the protocol, not
// serializer preferences: these types are declared in wire order, encoded
// with compact integers, and must never be replaced by Go maps (which encode
// in sorted, not declared, order).

// limitOrderWire is the "limit" branch of an order's type field. The adapter
// places no trigger orders, so no other branch exists.
type limitOrderWire struct {
	Tif string `msgpack:"tif" json:"tif"`
}

type orderTypeWire struct {
	Limit limitOrderWire `msgpack:"limit" json:"limit"`
}

// orderWire is one order in an order action. The abbreviated keys are the
// venue's: a asset, b isBuy, p price, s size, r reduceOnly, t type, c cloid.
type orderWire struct {
	Asset      int           `msgpack:"a" json:"a"`
	IsBuy      bool          `msgpack:"b" json:"b"`
	Price      string        `msgpack:"p" json:"p"`
	Size       string        `msgpack:"s" json:"s"`
	ReduceOnly bool          `msgpack:"r" json:"r"`
	OrderType  orderTypeWire `msgpack:"t" json:"t"`
	// Cloid is the client order id, assigned before submission. Omitted
	// entirely when unset — an empty string is a different preimage.
	Cloid string `msgpack:"c,omitempty" json:"c,omitempty"`
}

type orderAction struct {
	Type     string      `msgpack:"type" json:"type"`
	Orders   []orderWire `msgpack:"orders" json:"orders"`
	Grouping string      `msgpack:"grouping" json:"grouping"`
}

// cancelByCloidWire cancels by client order id rather than the venue's oid.
// The cloid is known before submission, so a cancel stays possible even when
// the placing response was lost.
type cancelByCloidWire struct {
	Asset int    `msgpack:"asset" json:"asset"`
	Cloid string `msgpack:"cloid" json:"cloid"`
}

type cancelByCloidAction struct {
	Type    string              `msgpack:"type" json:"type"`
	Cancels []cancelByCloidWire `msgpack:"cancels" json:"cancels"`
}
