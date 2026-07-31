package dedupe

import (
	"sync"
	"testing"
)

func TestObserveReportsFirstSightOnly(t *testing.T) {
	set := NewSet[string](4)

	if !set.Observe("a") {
		t.Fatal("first sight of a value reported as a repeat")
	}
	if set.Observe("a") {
		t.Error("a repeated value reported as new")
	}
	if !set.Observe("b") {
		t.Error("a distinct value reported as a repeat")
	}
}

// The ring forgets in insertion order, so capacity is what bounds the window a
// repeat is caught in — the value that survives eviction is the newest.
func TestObserveEvictsOldestFirst(t *testing.T) {
	set := NewSet[int](3)

	for value := range 4 {
		if !set.Observe(value) {
			t.Fatalf("Observe(%d) reported a repeat on first sight", value)
		}
	}
	// Checked before the evicted value, because observing that one re-inserts
	// it and evicts the next in line.
	for _, value := range []int{1, 2, 3} {
		if set.Observe(value) {
			t.Errorf("Observe(%d) reported new, but it is still within capacity", value)
		}
	}
	if !set.Observe(0) {
		t.Error("the oldest value was still remembered after eviction")
	}
}

// Observe is called from an adapter's submission path and from its account
// stream at once; exactly one caller may win a given value.
func TestObserveIsSafeUnderConcurrency(t *testing.T) {
	const callers = 16
	set := NewSet[string](128)

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
		mu    sync.Mutex
		won   int
	)
	start.Add(1)
	for range callers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			if set.Observe("contended") {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}()
	}
	start.Done()
	done.Wait()

	if won != 1 {
		t.Errorf("%d callers saw the value as new, want exactly 1", won)
	}
}

func TestNewSetRejectsNonPositiveCapacity(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewSet accepted a zero capacity, which can remember nothing")
		}
	}()
	NewSet[int](0)
}
