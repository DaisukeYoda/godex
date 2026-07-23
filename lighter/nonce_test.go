package lighter

import (
	"context"
	"errors"
	"testing"
)

func TestNonceManager(t *testing.T) {
	served := int64(7)
	manager := newNonceManager(func(context.Context) (int64, error) {
		return served, nil
	})

	// take before init fails fast.
	if _, err := manager.take(); err == nil {
		t.Fatal("expected error before init")
	}

	if err := manager.init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	for want := int64(7); want < 10; want++ {
		got, err := manager.take()
		if err != nil {
			t.Fatalf("take: %v", err)
		}
		if got != want {
			t.Fatalf("take = %d, want %d", got, want)
		}
	}

	// resync rebases on the server value.
	served = 42
	if err := manager.resync(context.Background()); err != nil {
		t.Fatalf("resync: %v", err)
	}
	if got, _ := manager.take(); got != 42 {
		t.Fatalf("take after resync = %d, want 42", got)
	}
}

func TestNonceManagerInitFailure(t *testing.T) {
	fetchErr := errors.New("boom")
	manager := newNonceManager(func(context.Context) (int64, error) {
		return 0, fetchErr
	})
	if err := manager.init(context.Background()); !errors.Is(err, fetchErr) {
		t.Fatalf("expected fetch error, got %v", err)
	}
	if _, err := manager.take(); err == nil {
		t.Fatal("failed init must leave the manager uninitialized")
	}
}
