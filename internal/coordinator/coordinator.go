package coordinator

import (
	"context"
	"fmt"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/ring"
)

// Coordinator routes client requests based on the current ring view.
type Coordinator struct {
	node      *node.Node
	ring      *ring.Ring
	forwarder *Forwarder
}

// New returns a Coordinator.
func New(n *node.Node, r *ring.Ring, f *Forwarder) *Coordinator {
	return &Coordinator{node: n, ring: r, forwarder: f}
}

// Put handles a put request.
// If this node is the primary for the key, it writes locally and
// asynchronously replicates to co-replicas.
// If this node is not a replica at all, it forwards to the primary.
func (c *Coordinator) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	primary, ok := c.ring.Primary(req.GetKey())
	if !ok {
		// Ring is empty (single node bootstrap or pre-gossip). Handle locally.
		return c.handleLocalPut(ctx, req)
	}

	localID := c.node.ReplicaID()

	if primary.ReplicaID == localID {
		// This node is the primary: handle locally then async-replicate.
		resp, err := c.handleLocalPut(ctx, req)
		if err != nil {
			return nil, err
		}
		go c.replicatePut(req, c.ring.GetReplicas(req.GetKey()), localID)
		return resp, nil
	}

	// This node is not the primary: forward to the primary.
	return c.forwarder.ForwardPut(ctx, primary.GRPCAddr, req)
}

// Get handles a get request.
// If this node is any replica for the key, serve locally.
// Otherwise, forward to the primary.
func (c *Coordinator) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	localID := c.node.ReplicaID()

	if c.ring.IsReplica(req.GetKey(), localID) {
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}

	primary, ok := c.ring.Primary(req.GetKey())
	if !ok {
		// Ring empty — serve locally.
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}
	return c.forwarder.ForwardGet(ctx, primary.GRPCAddr, req)
}

// Delete handles a delete request. Same routing logic as Put.
func (c *Coordinator) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	primary, ok := c.ring.Primary(req.GetKey())
	if !ok {
		return c.handleLocalDelete(ctx, req)
	}

	localID := c.node.ReplicaID()
	if primary.ReplicaID == localID {
		resp, err := c.handleLocalDelete(ctx, req)
		if err != nil {
			return nil, err
		}
		go c.replicateDelete(req, c.ring.GetReplicas(req.GetKey()), localID)
		return resp, nil
	}
	return c.forwarder.ForwardDelete(ctx, primary.GRPCAddr, req)
}

// ── local write helpers ───────────────────────────────────────────────────────

func (c *Coordinator) handleLocalPut(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ts, err := c.node.Put(req.GetKey(), req.GetValueJson())
	if err != nil {
		return nil, fmt.Errorf("coordinator: local put: %w", err)
	}
	return &kvpb.PutResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, nil
}

func (c *Coordinator) handleLocalDelete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ts, err := c.node.Delete(req.GetKey())
	if err != nil {
		return nil, fmt.Errorf("coordinator: local delete: %w", err)
	}
	return &kvpb.DeleteResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, nil
}

// ── async replication helpers ─────────────────────────────────────────────────

// replicatePut sends the put to all co-replicas except the local node.
// Errors are logged but do not affect the client response — anti-entropy
// corrects any missed replications during the next sync cycle.
func (c *Coordinator) replicatePut(req *kvpb.PutRequest, replicas []ring.Member, skipID string) {
	ctx := context.Background()
	for _, r := range replicas {
		if r.ReplicaID == skipID {
			continue
		}
		if _, err := c.forwarder.ForwardPut(ctx, r.GRPCAddr, req); err != nil {
			_ = err // anti-entropy will reconcile
		}
	}
}

func (c *Coordinator) replicateDelete(req *kvpb.DeleteRequest, replicas []ring.Member, skipID string) {
	ctx := context.Background()
	for _, r := range replicas {
		if r.ReplicaID == skipID {
			continue
		}
		if _, err := c.forwarder.ForwardDelete(ctx, r.GRPCAddr, req); err != nil {
			_ = err
		}
	}
}
