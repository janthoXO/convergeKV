package coordinator

import (
	"context"
	"time"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/connpool"
)

const defaultForwardTimeout = 2 * time.Second

// Forwarder forwards client requests to peer nodes over gRPC.
type Forwarder struct {
	pool    *connpool.Pool
	timeout time.Duration
}

// ForwarderOption configures a Forwarder.
type ForwarderOption func(*Forwarder)

// WithForwardTimeout sets the per-call deadline applied to each forwarded RPC.
// The inbound request context is still honoured as the parent; this timeout
// adds a floor to prevent hangs when a peer is slow or unreachable.
func WithForwardTimeout(d time.Duration) ForwarderOption {
	return func(f *Forwarder) { f.timeout = d }
}

// NewForwarder returns a Forwarder backed by the given shared connection pool.
func NewForwarder(pool *connpool.Pool, opts ...ForwarderOption) *Forwarder {
	f := &Forwarder{pool: pool, timeout: defaultForwardTimeout}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *Forwarder) clientFor(addr string) (fwdpb.ForwardServiceClient, error) {
	conn, err := f.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return fwdpb.NewForwardServiceClient(conn), nil
}

// ForwardPut forwards a put request to the node at addr.
func (f *Forwarder) ForwardPut(ctx context.Context, addr string, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	return client.ForwardPut(ctx, req)
}

// ForwardGet forwards a get request to the node at addr.
func (f *Forwarder) ForwardGet(ctx context.Context, addr string, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	return client.ForwardGet(ctx, req)
}

// ForwardDelete forwards a delete request to the node at addr.
func (f *Forwarder) ForwardDelete(ctx context.Context, addr string, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	return client.ForwardDelete(ctx, req)
}
