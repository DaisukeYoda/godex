// Package dispatch tells whether an HTTP request ever reached the wire. The
// venue adapters need that one bit to classify a failed submission: a request
// that never left the process was not applied and is a plain failure, while
// one whose bytes went out may have been applied however the call ended — a
// timeout, a dropped connection, a Close — and is an unknown outcome that must
// be reconciled, never retried.
package dispatch

import (
	"context"
	"net/http/httptrace"
	"sync/atomic"
)

// Trace returns ctx wired to observe the request made with it, and a
// function reporting whether any of that request's bytes reached the wire.
// The boundary is the first byte of headers written: from then on the server
// may have read the request, so a failure afterwards is ambiguous. Nothing
// written — the context already canceled, a dial that failed, a stale
// connection detected before the write — is not.
func Trace(ctx context.Context) (context.Context, func() bool) {
	var wrote atomic.Bool
	traced := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { wrote.Store(true) },
	})
	return traced, wrote.Load
}
