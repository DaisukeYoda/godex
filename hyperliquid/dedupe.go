package hyperliquid

// fillCacheSize bounds the remembered trade ids. The venue's userFills
// snapshot replays at most a couple of thousand recent fills, so a cache
// several times that size cannot evict an id that is still replayable.
const fillCacheSize = 8192

// fillCache remembers trade ids this executor has already reported, so a
// reconnect's userFills snapshot republishes nothing. It is a fixed-size
// ring: the oldest id is evicted once the cache is full.
type fillCache struct {
	seen map[int64]struct{}
	ring []int64
	next int
}

func newFillCache() *fillCache {
	return &fillCache{
		seen: make(map[int64]struct{}, fillCacheSize),
		ring: make([]int64, 0, fillCacheSize),
	}
}

// observe records a trade id and reports whether it is new. A repeated id
// returns false and must not be emitted again.
func (c *fillCache) observe(tradeID int64) bool {
	if _, present := c.seen[tradeID]; present {
		return false
	}
	if len(c.ring) < fillCacheSize {
		c.ring = append(c.ring, tradeID)
	} else {
		delete(c.seen, c.ring[c.next])
		c.ring[c.next] = tradeID
		c.next = (c.next + 1) % fillCacheSize
	}
	c.seen[tradeID] = struct{}{}
	return true
}
