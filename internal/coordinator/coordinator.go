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
	return &Coordinator{node: n, gossip: g, forwarder: f, syncer: syncer, rf: rf}
}

// Put handles a put request.
// If the local node is an HRW replica for the key, it writes locally and
// asynchronously pushes to the other HRW replicas.
// Otherwise it forwards to the highest-scoring HRW member.
func (c *Coordinator) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || containsID(replicas, localID) {
		resp, err := c.handleLocalPut(ctx, req)
		if err != nil {
			return nil, err
		}
		if c.syncer != nil {
			// Build the DeltaEntry list for push (one entry per field).
			// The entries were just written; pull them from the node snapshot.
			go c.pushWriteToPeers(ctx, req.GetKey(), replicas, localID)
		}
		return resp, nil
	}

	// Forward to the highest scorer.
	target := hrw.HighestScorer(req.GetKey(), members)
	if target.GRPCAddr == "" {
		// No reachable target — serve locally as fallback.
		return c.handleLocalPut(ctx, req)
	}
	return c.forwarder.ForwardPut(ctx, target.GRPCAddr, req)
}

// Get handles a get request.
// Serves locally if the local node is an HRW replica; otherwise forwards.
func (c *Coordinator) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || containsID(replicas, localID) {
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}

	target := hrw.HighestScorer(req.GetKey(), members)
	if target.GRPCAddr == "" {
		v, found := c.node.Get(req.GetKey())
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}
	return c.forwarder.ForwardGet(ctx, target.GRPCAddr, req)
}

// Delete handles a delete request. Same routing logic as Put.
func (c *Coordinator) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || containsID(replicas, localID) {
		resp, err := c.handleLocalDelete(ctx, req)
		if err != nil {
			return nil, err
		}
		if c.syncer != nil {
			go c.pushWriteToPeers(ctx, req.GetKey(), replicas, localID)
		}
		return resp, nil
	}

	target := hrw.HighestScorer(req.GetKey(), members)
	if target.GRPCAddr == "" {
		return c.handleLocalDelete(ctx, req)
	}
	return c.forwarder.ForwardDelete(ctx, target.GRPCAddr, req)
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

// pushWriteToPeers collects the current entries for key from the node snapshot
// and pushes them to the other HRW replicas via PushEntries.
// Called asynchronously from the write path; errors are logged but not retried.
func (c *Coordinator) pushWriteToPeers(ctx context.Context, key string, replicas []gossip.MemberInfo, localID string) {
	snap := c.node.Snapshot()
	var entries []*repb.DeltaEntry
	for _, r := range snap {
		if r.Key != key {
			continue
		}
		entries = append(entries, &repb.DeltaEntry{
			Key:       r.Key,
			Field:     r.Field,
			ValueJson: r.Entry.Value,
			Timestamp: &kvpb.HLCTimestamp{
				PhysicalMs: r.Entry.Timestamp.PhysicalMs,
				Logical:    r.Entry.Timestamp.Logical,
			},
			ReplicaId: r.Entry.ReplicaID,
			Deleted:   r.Entry.Deleted,
		})
	}
	if len(entries) == 0 {
		log.Printf("[coordinator] pushWriteToPeers: no entries for key %s (already applied?)", key)
		return
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
