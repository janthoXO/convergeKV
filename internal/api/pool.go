package api

import (
	"context"
	"errors"
	"io"
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

// --- antientropy.Peer ---------------------------------------------------------

func (p *Pool) MerkleRoot(ctx context.Context, addr string, pid uint16) ([]byte, error) {
	c, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := pb.NewNodeClient(c).MerkleRoot(ctx, &pb.MerkleRootRequest{Partition: uint32(pid)})
	if err != nil {
		return nil, err
	}
	return resp.GetRoot(), nil
}

func (p *Pool) MerkleLeaves(ctx context.Context, addr string, pid uint16) ([]byte, error) {
	c, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := pb.NewNodeClient(c).MerkleLeaves(ctx, &pb.MerkleLeavesRequest{Partition: uint32(pid)})
	if err != nil {
		return nil, err
	}
	return resp.GetLeaves(), nil
}

func (p *Pool) SyncBucket(ctx context.Context, addr string, pid, bucket uint16, fn func(key, doc []byte) error) error {
	c, err := p.conn(addr)
	if err != nil {
		return err
	}
	stream, err := pb.NewNodeClient(c).SyncBucket(ctx,
		&pb.SyncBucketRequest{Partition: uint32(pid), Bucket: uint32(bucket)},
		grpc.UseCompressor(Zstd))
	if err != nil {
		return err
	}
	for {
		doc, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(doc.GetKey(), doc.GetDocument()); err != nil {
			return err
		}
	}
}

// Snapshot implements transfer.Source.
func (p *Pool) Snapshot(ctx context.Context, addr string, pid uint16, fn func(key, doc []byte) error) error {
	c, err := p.conn(addr)
	if err != nil {
		return err
	}
	stream, err := pb.NewNodeClient(c).Snapshot(ctx,
		&pb.SnapshotRequest{Partition: uint32(pid)}, grpc.UseCompressor(Zstd))
	if err != nil {
		return err
	}
	for {
		doc, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(doc.GetKey(), doc.GetDocument()); err != nil {
			return err
		}
	}
}

func (p *Pool) ApplyDelta(ctx context.Context, addr string, pid uint16, key, delta []byte) error {
	return p.SendDelta(ctx, addr, replication.Delta{Partition: pid, Key: key, Delta: delta})
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
