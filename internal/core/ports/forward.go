package ports

import (
	"context"

	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// Forwarder is the port the coordinator uses to forward a request to a peer
// replica when the local node does not own the key's partition. The concrete
// implementation dials the peer's ForwardService over gRPC; the coordinator
// only sees domain types, never protobuf.
type Forwarder interface {
	ForwardPut(ctx context.Context, addr, key, valueJSON string) (hlc.Timestamp, error)
	ForwardGet(ctx context.Context, addr, key string) (value string, found bool, err error)
	ForwardDelete(ctx context.Context, addr, key string) (hlc.Timestamp, error)
}
