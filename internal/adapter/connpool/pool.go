// Package connpool provides a thread-safe, lazily-connected gRPC connection pool.
package connpool

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool is a thread-safe, lazily-connected gRPC connection pool.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// New returns an empty Pool.
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

// Evict closes and removes the connection for each addr, if present.
func (p *Pool) Evict(addr ...string) {
	p.mu.Lock()
	var toClose []*grpc.ClientConn
	for _, a := range addr {
		if c, ok := p.conns[a]; ok {
			toClose = append(toClose, c)
			delete(p.conns, a)
		}
	}
	p.mu.Unlock()
	for _, c := range toClose {
		c.Close()
	}
}

// EvictAbsent closes connections for every address not in keep.
func (p *Pool) EvictAbsent(keep map[string]struct{}) {
	p.mu.Lock()
	var toClose []*grpc.ClientConn
	for addr, c := range p.conns {
		if _, ok := keep[addr]; !ok {
			toClose = append(toClose, c)
			delete(p.conns, addr)
		}
	}
	p.mu.Unlock()
	for _, c := range toClose {
		c.Close()
	}
}

// Close shuts down all pooled connections.
func (p *Pool) Close() {
	p.mu.Lock()
	toClose := make([]*grpc.ClientConn, 0, len(p.conns))
	for _, c := range p.conns {
		toClose = append(toClose, c)
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.mu.Unlock()
	for _, c := range toClose {
		c.Close()
	}
}
