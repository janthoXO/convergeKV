package coordinator

import (
	"context"
	"fmt"
	"log"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hrw"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// PushSyncer is the minimal interface the coordinator needs from the syncer
// for write-path push. Avoids a direct import of the syncer package.
type PushSyncer interface {
	PushToPeers(ctx context.Context, entries []*repb.DeltaEntry, replicas []gossip.MemberInfo, localID string)
}

// Coordinator routes client requests using Rendezvous Hashing (HRW).
// Any replica in the HRW set for a key serves reads and writes (quorum=1).
// If the local node is not a replica, the request is forwarded to the
// highest-scoring HRW member for deterministic routing.
type Coordinator struct {
	node      *node.Node
	gossip    *gossip.Gossip
	forwarder *Forwarder
	syncer    PushSyncer
	rf        int
}

// New returns a Coordinator.
func New(n *node.Node, g *gossip.Gossip, f *Forwarder, syncer PushSyncer, rf int) *Coordinator {
	c := &Coordinator{node: n, gossip: g, forwarder: f, syncer: syncer, rf: rf}
	return c
}

// Put handles a put request.
// If the local node is an HRW replica for the key, it writes locally and
// asynchronously pushes to the other HRW replicas.
// Otherwise it forwards to the highest-scoring HRW member.
func (c *Coordinator) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if containsID(replicas, localID) {
		resp, updates, err := c.handleLocalPut(ctx, req)
		if err != nil {
			return nil, err
		}
		c.pushWriteToPeers(ctx, updates, replicas, localID)
		return resp, nil
	}

	return forwardWithRetry(ctx, replicas, func(addr string) (*kvpb.PutResponse, error) {
		return c.forwarder.ForwardPut(ctx, addr, req)
	})
}

// Get handles a get request.
// Serves locally if the local node is an HRW replica; otherwise forwards.
func (c *Coordinator) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if containsID(replicas, localID) {
		v, found, err := c.node.Get(req.GetKey())
		if err != nil {
			return nil, fmt.Errorf("coordinator: local get: %w", err)
		}
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}

	return forwardWithRetry(ctx, replicas, func(addr string) (*kvpb.GetResponse, error) {
		return c.forwarder.ForwardGet(ctx, addr, req)
	})
}

// Delete handles a delete request. Same routing logic as Put.
func (c *Coordinator) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if containsID(replicas, localID) {
		resp, updates, err := c.handleLocalDelete(ctx, req)
		if err != nil {
			return nil, err
		}
		c.pushWriteToPeers(ctx, updates, replicas, localID)
		return resp, nil
	}

	return forwardWithRetry(ctx, replicas, func(addr string) (*kvpb.DeleteResponse, error) {
		return c.forwarder.ForwardDelete(ctx, addr, req)
	})
}

// ── local write helpers ───────────────────────────────────────────────────────

func (c *Coordinator) handleLocalPut(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, []storage.FieldUpdate, error) {
	ts, updates, err := c.node.Put(req.GetKey(), req.GetValueJson())
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: local put: %w", err)
	}
	return &kvpb.PutResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, updates, nil
}

func (c *Coordinator) handleLocalDelete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, []storage.FieldUpdate, error) {
	ts, updates, err := c.node.Delete(req.GetKey())
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: local delete: %w", err)
	}
	return &kvpb.DeleteResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, updates, nil
}

// pushWriteToPeers converts the written FieldUpdates to DeltaEntries and fans
// them out to the other HRW replicas. The syncer owns goroutine tracking and timeouts.
func (c *Coordinator) pushWriteToPeers(ctx context.Context, updates []storage.FieldUpdate, replicas []gossip.MemberInfo, localID string) {
	if len(updates) == 0 {
		return
	}
	entries := make([]*repb.DeltaEntry, 0, len(updates))
	for _, u := range updates {
		entries = append(entries, &repb.DeltaEntry{
			Key:       u.Key,
			Field:     u.Field,
			ValueJson: u.Entry.Value,
			Timestamp: &kvpb.HLCTimestamp{
				PhysicalMs: u.Entry.Timestamp.PhysicalMs,
				Logical:    u.Entry.Timestamp.Logical,
			},
			ReplicaId: u.Entry.ReplicaID,
			Deleted:   u.Entry.Deleted,
		})
	}
	c.syncer.PushToPeers(ctx, entries, replicas, localID)
}

// containsID reports whether any member in the slice has ReplicaID == id.
func containsID(members []gossip.MemberInfo, id string) bool {
	for _, m := range members {
		if m.ReplicaID == id {
			return true
		}
	}
	return false
}

// forwardWithRetry tries each replica in HRW-score order, returning on the
// first success. If all attempts fail, the last error is returned.
// This tolerates peers that gossip has not yet evicted after a crash.
func forwardWithRetry[R any](ctx context.Context, replicas []gossip.MemberInfo, fn func(addr string) (R, error)) (R, error) {
	var lastErr error
	for _, m := range replicas {
		if ctx.Err() != nil {
			break
		}
		if result, err := fn(m.GRPCAddr); err == nil {
			return result, nil
		} else {
			log.Printf("[coordinator] forward to %s failed, trying next replica: %v", m.GRPCAddr, err)
			lastErr = err
		}
	}

	var zero R
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return zero, lastErr
}
