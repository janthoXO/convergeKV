// Package forward implements ports.Forwarder by dialing a peer's
// ForwardService over gRPC. It is the only place in the system that maps the
// coordinator's domain-typed forwarding calls onto gen/kv and gen/forward
// protobuf messages.
package forward

import (
	"context"
	"time"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

const defaultForwardTimeout = 2 * time.Second

// Forwarder forwards client requests to peer nodes over gRPC.
// Satisfies ports.Forwarder.
type Forwarder struct {
	pool    *connpool.Pool
	timeout time.Duration
}

// Option configures a Forwarder.
type Option func(*Forwarder)

// WithForwardTimeout sets the per-call deadline applied to each forwarded RPC.
func WithForwardTimeout(d time.Duration) Option {
	return func(f *Forwarder) { f.timeout = d }
}

// New returns a Forwarder backed by the given shared connection pool.
func New(pool *connpool.Pool, opts ...Option) *Forwarder {
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
func (f *Forwarder) ForwardPut(ctx context.Context, addr, key, valueJSON string) (hlc.Timestamp, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	resp, err := client.ForwardPut(ctx, &kvpb.PutRequest{Key: key, ValueJson: valueJSON})
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return reproto.ProtoToHLC(resp.GetTimestamp()), nil
}

// ForwardGet forwards a get request to the node at addr.
func (f *Forwarder) ForwardGet(ctx context.Context, addr, key string) (string, bool, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return "", false, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	resp, err := client.ForwardGet(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		return "", false, err
	}
	return resp.GetValueJson(), resp.GetFound(), nil
}

// ForwardDelete forwards a delete request to the node at addr.
func (f *Forwarder) ForwardDelete(ctx context.Context, addr, key string) (hlc.Timestamp, error) {
	client, err := f.clientFor(addr)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	resp, err := client.ForwardDelete(ctx, &kvpb.DeleteRequest{Key: key})
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return reproto.ProtoToHLC(resp.GetTimestamp()), nil
}
