package lighter

import (
	"context"
	"fmt"
)

// nonceManager tracks the next Lighter transaction nonce. Nonces are strictly
// increasing per api_key_index; drift gets transactions rejected.
//
//   - init loads the initial value from GET /nextNonce; take then increments
//     locally.
//   - When sendTx reports an API rejection, the caller resyncs from REST
//     (rejected transactions do not advance the server nonce).
//
// Not safe for concurrent use on its own: every access must happen under the
// executor's transaction mutex, which also guarantees that nonce allocation
// order equals submission order.
type nonceManager struct {
	fetchNextNonce func(ctx context.Context) (int64, error)
	next           int64
	initialized    bool
}

func newNonceManager(fetchNextNonce func(ctx context.Context) (int64, error)) *nonceManager {
	return &nonceManager{fetchNextNonce: fetchNextNonce}
}

func (n *nonceManager) init(ctx context.Context) error {
	return n.resync(ctx)
}

func (n *nonceManager) take() (int64, error) {
	if !n.initialized {
		return 0, fmt.Errorf("lighter: nonce manager is not initialized")
	}
	nonce := n.next
	n.next++
	return nonce, nil
}

// resync refetches the next nonce from REST (used at init and after any API
// rejection or unknown-outcome recovery).
func (n *nonceManager) resync(ctx context.Context) error {
	next, err := n.fetchNextNonce(ctx)
	if err != nil {
		return err
	}
	n.next = next
	n.initialized = true
	return nil
}
