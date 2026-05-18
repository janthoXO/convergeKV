package connpool

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool is a thread-safe, lazily-connected gRPC connection pool.
// Evict or EvictAbsent must be called when peers depart to avoid
// accumulating dead connections indefinitely.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func New() *Pool { return &Pool{conns: make(map[string]*grpc.ClientConn)} }

// Get returns the existing connection for addr or dials a new one.
func (p *Pool) Get(addr string) (*grpc.ClientConn, error) {
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

// Evict closes and removes the connection for addr, if present.
func (p *Pool) Evict(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		c.Close()
		delete(p.conns, addr)
	}
}

// EvictAbsent closes connections for every address not in keep.
// Call this on every gossip membership snapshot to drop departed nodes.
func (p *Pool) EvictAbsent(keep map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		if _, ok := keep[addr]; !ok {
			c.Close()
			delete(p.conns, addr)
		}
	}
}

// Close shuts down all pooled connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}
