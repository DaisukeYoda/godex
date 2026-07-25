// Package pb holds the generated protobuf types the dydx adapter needs to
// build, sign, and broadcast dYdX v4 (Cosmos SDK) transactions.
//
// The .proto sources under proto/ are hand-trimmed, wire-compatible subsets of
// the upstream definitions in dydxprotocol/v4-chain and cosmos-sdk. Only the
// messages godex actually encodes or decodes are declared, and every field
// carries its upstream number and type, so the bytes on the wire are identical
// to what the chain's own codec produces. Vendoring these few messages avoids
// depending on github.com/dydxprotocol/v4-chain/protocol, whose go.mod pins
// forked cosmos-sdk, cometbft, iavl, and ibc-go via replace directives that Go
// does not honor transitively.
//
// Regenerate after editing any .proto (buf compiles without protoc):
//
//	go install github.com/bufbuild/buf/cmd/buf@v1.47.2
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
//	cd dydx/internal/pb && buf generate
//
// Generated .pb.go files are committed so an ordinary `go build` needs no
// protobuf toolchain.
package pb
