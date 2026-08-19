// Package dispatchtest provides an http.RoundTripper that fails selected
// requests before anything reaches the wire — a dial that never happened —
// so tests can prove such a failure is classified as "not dispatched".
package dispatchtest

import (
	"errors"
	"net/http"
	"sync/atomic"
)

// ErrNotDialed is the error the transport returns for a request it refuses.
var ErrNotDialed = errors.New("dispatchtest: connection refused before dial")

// FailBeforeWire returns a RoundTripper that answers the first times requests
// matching match with ErrNotDialed without dialing, and forwards everything
// else to next.
func FailBeforeWire(next http.RoundTripper, match func(*http.Request) bool, times int) http.RoundTripper {
	transport := &failBeforeWire{next: next, match: match}
	transport.remaining.Store(int32(times))
	return transport
}

type failBeforeWire struct {
	next      http.RoundTripper
	match     func(*http.Request) bool
	remaining atomic.Int32
}

func (f *failBeforeWire) RoundTrip(request *http.Request) (*http.Response, error) {
	if f.match(request) && f.remaining.Add(-1) >= 0 {
		return nil, ErrNotDialed
	}
	return f.next.RoundTrip(request)
}
