// Package dedupe bounds the "have I already reported this?" state an adapter
// needs to keep an at-most-once event contract.
package dedupe

import "sync"

// RejectionCapacity bounds the remembered rejected order ids. A repeat is
// only possible while both reports of one order's fate are still in flight —
// the venue's answer to the submission and the same outcome pushed over the
// account stream — plus whatever an account snapshot replays after a
// reconnect. Both windows are far shorter than the several thousand orders it
// takes to evict an id here.
const RejectionCapacity = 8192

// Set remembers the values it has been shown, up to a fixed capacity, and
// reports whether each one is new. It is a ring: once full, the oldest value
// is forgotten, so a value first seen long enough ago can be reported twice.
// Capacity has to outlast the window in which a repeat is possible, not the
// executor's whole life.
//
// Safe for concurrent use. The lock is a leaf — Observe calls nothing — so it
// may be taken while an adapter holds its own state lock.
type Set[T comparable] struct {
	mu       sync.Mutex
	seen     map[T]struct{}
	ring     []T
	next     int
	capacity int
}

// NewSet returns a Set that remembers at most capacity values.
func NewSet[T comparable](capacity int) *Set[T] {
	if capacity <= 0 {
		panic("dedupe: capacity must be positive")
	}
	return &Set[T]{
		seen:     make(map[T]struct{}, capacity),
		ring:     make([]T, 0, capacity),
		capacity: capacity,
	}
}

// Observe records a value and reports whether it is new. A repeat returns
// false and must not be reported again.
func (s *Set[T]) Observe(value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, present := s.seen[value]; present {
		return false
	}
	if len(s.ring) < s.capacity {
		s.ring = append(s.ring, value)
	} else {
		delete(s.seen, s.ring[s.next])
		s.ring[s.next] = value
		s.next = (s.next + 1) % s.capacity
	}
	s.seen[value] = struct{}{}
	return true
}
