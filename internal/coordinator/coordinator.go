package coordinator

import (
	"context"
	"fmt"
	"log"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/partition"
	utils "github.com/janthoXO/convergeKV/internal/utils"
)

// Coordinator routes client requests based on the current slot map.
type Coordinator struct {
	node      *node.Node
	slotMap   func() partition.SlotMap // returns the current slot map snapshot
	forwarder *Forwarder
	grpcAddrs func(replicaID string) (string, bool) // resolves replica ID → gRPC address
}

// New returns a Coordinator.
// getSlotMap must return the latest SlotMap on every call (e.g. via an atomic load).
// resolveAddr must map a replicaID to its host:grpcPort address.
func New(n *node.Node, getSlotMap func() partition.SlotMap, resolveAddr func(string) (string, bool), f *Forwarder) *Coordinator {
	return &Coordinator{node: n, slotMap: getSlotMap, grpcAddrs: resolveAddr, forwarder: f}
}

// Put handles a put request.
// If this node is any replica for the key's slot, it writes locally and
// asynchronously pushes to co-replicas. Otherwise forwards to replicas[0].
func (c *Coordinator) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	replicas := c.slotMap().ReplicasForKey(req.GetKey())
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || utils.Contains(replicas, localID) {
		// Handle locally and push to co-replicas.
		resp, err := c.handleLocalPut(ctx, req)
		if err != nil {
			return nil, err
		}
		go c.pushPut(req, replicas, localID)
		return resp, nil
	}

	// Forward to the first replica in the slot (deterministic, any is correct).
	addr, ok := c.grpcAddrs(replicas[0])
	if !ok {
		// Address unknown — handle locally as a fallback (pre-convergence).
		return c.handleLocalPut(ctx, req)
	}
	return c.forwarder.ForwardPut(ctx, addr, req)
}

// Get handles a get request.
// Serves locally if this node is any replica for the slot; otherwise forwards.
func (c *Coordinator) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	replicas := c.slotMap().ReplicasForKey(req.GetKey())
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || utils.Contains(replicas, localID) {
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}

	addr, ok := c.grpcAddrs(replicas[0])
	if !ok {
		// Serve locally as fallback.
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}
	return c.forwarder.ForwardGet(ctx, addr, req)
}

// Delete handles a delete request. Same routing logic as Put.
func (c *Coordinator) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	replicas := c.slotMap().ReplicasForKey(req.GetKey())
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || utils.Contains(replicas, localID) {
		resp, err := c.handleLocalDelete(ctx, req)
		if err != nil {
			return nil, err
		}
		go c.pushDelete(req, replicas, localID)
		return resp, nil
	}

	addr, ok := c.grpcAddrs(replicas[0])
	if !ok {
		return c.handleLocalDelete(ctx, req)
	}
	return c.forwarder.ForwardDelete(ctx, addr, req)
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

// ── async push helpers ────────────────────────────────────────────────────────

// pushPut sends the put to all co-replicas except the local node.
// Errors are logged but do not affect the client response — anti-entropy
// corrects any missed replications during the next sync cycle.
func (c *Coordinator) pushPut(req *kvpb.PutRequest, replicas []string, skipID string) {
	ctx := context.Background()
	for _, replicaID := range replicas {
		if replicaID == skipID {
			continue
		}
		addr, ok := c.grpcAddrs(replicaID)
		if !ok {
			log.Printf("[coordinator] pushPut: unknown addr for replica %s", replicaID)
			continue
		}
		if _, err := c.forwarder.ForwardPut(ctx, addr, req); err != nil {
			log.Printf("[coordinator] pushPut to %s: %v (anti-entropy will reconcile)", replicaID, err)
		}
	}
}

func (c *Coordinator) pushDelete(req *kvpb.DeleteRequest, replicas []string, skipID string) {
	ctx := context.Background()
	for _, replicaID := range replicas {
		if replicaID == skipID {
			continue
		}
		addr, ok := c.grpcAddrs(replicaID)
		if !ok {
			log.Printf("[coordinator] pushDelete: unknown addr for replica %s", replicaID)
			continue
		}
		if _, err := c.forwarder.ForwardDelete(ctx, addr, req); err != nil {
			log.Printf("[coordinator] pushDelete to %s: %v (anti-entropy will reconcile)", replicaID, err)
		}
	}
}
