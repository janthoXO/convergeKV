package coordinator

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
)

// Forwarder maintains a pool of gRPC connections to peer nodes and
// provides methods to forward client requests to them.
type Forwarder struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // keyed by grpc addr
}

// NewForwarder returns an empty Forwarder.
func NewForwarder() *Forwarder {
	return &Forwarder{conns: make(map[string]*grpc.ClientConn)}
}

// Close shuts down all pooled connections.
func (f *Forwarder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
}

func (f *Forwarder) clientFor(addr string) (fwdpb.ForwardServiceClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if conn, ok := f.conns[addr]; ok {
		return fwdpb.NewForwardServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	f.conns[addr] = conn
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
