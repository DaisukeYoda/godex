package txflow

import "github.com/DaisukeYoda/godex"

// oidIndexCapacity bounds the remembered oid → order id pairs. Fills are read
// by polling and may land after the order they belong to has ended, so the
// pairing must outlive order tracking; several thousand orders is far more
// than can end between two polls.
const oidIndexCapacity = 8192

// oidIndex maps venue oids to the executor's order ids, including for orders
// that have already ended. The venue attaches no client id to fills, so this
// is the only way a fill is attributed. It is a fixed-size ring: the oldest
// pair is evicted once the index is full.
type oidIndex struct {
	byOid map[int64]godex.OrderID
	ring  []int64
	next  int
}

func newOidIndex() *oidIndex {
	return &oidIndex{
		byOid: make(map[int64]godex.OrderID, oidIndexCapacity),
		ring:  make([]int64, 0, oidIndexCapacity),
	}
}

func (x *oidIndex) bind(oid int64, id godex.OrderID) {
	if _, present := x.byOid[oid]; present {
		x.byOid[oid] = id
		return
	}
	if len(x.ring) < oidIndexCapacity {
		x.ring = append(x.ring, oid)
	} else {
		delete(x.byOid, x.ring[x.next])
		x.ring[x.next] = oid
		x.next = (x.next + 1) % oidIndexCapacity
	}
	x.byOid[oid] = id
}

func (x *oidIndex) lookup(oid int64) (godex.OrderID, bool) {
	id, ok := x.byOid[oid]
	return id, ok
}
