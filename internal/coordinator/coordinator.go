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
	store     *storage.Store
	rf        int
}

// New returns a Coordinator.
func New(n *node.Node, g *gossip.Gossip, f *Forwarder, syncer PushSyncer, store *storage.Store, rf int) *Coordinator {
	return &Coordinator{node: n, gossip: g, forwarder: f, syncer: syncer, store: store, rf: rf}
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
		go c.pushWriteToPeers(context.Background(), req.GetKey(), replicas, localID)
		return resp, nil
	}

	// Forward to the highest-scoring HRW member.
	target := hrw.HighestScorer(req.GetKey(), members)
	return c.forwarder.ForwardPut(ctx, target.GRPCAddr, req)
}

// Get handles a get request.
// Serves locally if the local node is an HRW replica; otherwise forwards.
func (c *Coordinator) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	members := c.gossip.Members()
	replicas := hrw.Replicas(req.GetKey(), members, c.rf)
	localID := c.node.ReplicaID()

	if len(replicas) == 0 || containsID(replicas, localID) {
		v, found, err := c.node.Get(req.GetKey())
		if err != nil {
			return nil, fmt.Errorf("coordinator: local get: %w", err)
		}
		return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
	}

	target := hrw.HighestScorer(req.GetKey(), members)
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
		go c.pushWriteToPeers(context.Background(), req.GetKey(), replicas, localID)
		return resp, nil
	}

	target := hrw.HighestScorer(req.GetKey(), members)
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

// pushWriteToPeers reads the current entries for key from Badger and pushes
// them to the other HRW replicas via PushEntries.
// Called asynchronously from the write path; errors are logged but not retried.
func (c *Coordinator) pushWriteToPeers(ctx context.Context, key string, replicas []gossip.MemberInfo, localID string) {
	m, err := c.store.GetKey(key)
	if err != nil {
		log.Printf("[coordinator] pushWriteToPeers: GetKey %s: %v", key, err)
		return
	}

	var entries []*repb.DeltaEntry
	for field, e := range m.Fields {
		entries = append(entries, &repb.DeltaEntry{
			Key:       key,
			Field:     field,
			ValueJson: e.Value,
			Timestamp: &kvpb.HLCTimestamp{
				PhysicalMs: e.Timestamp.PhysicalMs,
				Logical:    e.Timestamp.Logical,
			},
			ReplicaId: e.ReplicaID,
			Deleted:   e.Deleted,
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
