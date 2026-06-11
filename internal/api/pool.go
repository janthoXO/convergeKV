package api

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/janthoXO/convergeKV/internal/replication"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// Pool lazily maintains one gRPC client connection per peer address.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewPool() *Pool {
	return &Pool{conns: make(map[string]*grpc.ClientConn)}
}

func (p *Pool) conn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// Forward implements coordinator.Forwarder.
func (p *Pool) Forward(ctx context.Context, addr string, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	c, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewNodeClient(c).Forward(ctx, req)
}

// SendDelta implements replication.SendFunc.
func (p *Pool) SendDelta(ctx context.Context, addr string, d replication.Delta) error {
	c, err := p.conn(addr)
	if err != nil {
		return err
	}
	_, err = pb.NewNodeClient(c).ApplyDelta(ctx, &pb.ApplyDeltaRequest{
		Partition: uint32(d.Partition),
		Key:       d.Key,
		Delta:     d.Delta,
	})
	return err
}
