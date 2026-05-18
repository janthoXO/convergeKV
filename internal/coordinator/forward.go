package coordinator

import (
	"context"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/connpool"
)

// Forwarder forwards client requests to peer nodes over gRPC.
type Forwarder struct {
	pool *connpool.Pool
}

// NewForwarder returns a Forwarder backed by the given shared connection pool.
func NewForwarder(pool *connpool.Pool) *Forwarder {
	return &Forwarder{pool: pool}
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
	return client.ForwardPut(ctx, req)
}

// ForwardGet forwards a get request to the node at addr.
func (f *Forwarder) ForwardGet(ctx context.Context, addr string, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return nil, err
	}
	return client.ForwardGet(ctx, req)
}

// ForwardDelete forwards a delete request to the node at addr.
func (f *Forwarder) ForwardDelete(ctx context.Context, addr string, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return nil, err
	}
	return client.ForwardDelete(ctx, req)
}
