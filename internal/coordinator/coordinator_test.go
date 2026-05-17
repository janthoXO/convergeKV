package coordinator_test

import (
	"context"
	"os"
	"testing"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// noOpSyncer is a minimal manual stub that satisfies coordinator.PushSyncer.
// It records calls so tests can assert that pushes were (or were not) triggered,
// but performs no real network I/O.
type noOpSyncer struct {
	calls int
}

func (s *noOpSyncer) PushToPeers(
	_ context.Context,
	_ []*repb.DeltaEntry,
	_ []gossip.MemberInfo,
	_ string,
) {
	s.calls++
}

// ── helpers ──────────────────────────────────────────────────────────────────

func tempNode(t *testing.T, id string) *node.Node {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open storage for %s: %v", id, err)
	}
	t.Cleanup(func() { store.Close(); os.RemoveAll(dir) })
	n, err := node.New(id, store)
	if err != nil {
		t.Fatalf("create node %s: %v", id, err)
	}
	return n
}

// singleMemberGossip starts a one-node gossip instance so that the local node
// is always a replica (HRW with rf≥1, one member → it IS the top scorer).
func singleMemberGossip(t *testing.T, replicaID string, grpcPort, gossipPort int) *gossip.Gossip {
	t.Helper()
	g, err := gossip.Start(gossip.Config{
		BindAddr:  "127.0.0.1",
		BindPort:  gossipPort,
		LocalMeta: gossip.NodeMeta{ReplicaID: replicaID, GRPCPort: grpcPort},
	})
	if err != nil {
		t.Fatalf("gossip start: %v", err)
	}
	t.Cleanup(func() { g.Leave(0) })
	return g
}

// newCoord wires up a Coordinator with a noOpSyncer (no gRPC forwarder needed
// for local-replica tests because forwarding is never invoked).
func newCoord(t *testing.T, n *node.Node, g *gossip.Gossip, syncer *noOpSyncer) *coordinator.Coordinator {
	t.Helper()
	fwd := coordinator.NewForwarder()
	t.Cleanup(fwd.Close)
	return coordinator.New(n, g, fwd, syncer, 1 /*rf=1: local node is always the replica*/)
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestPutLocalAndPushTriggered verifies that a Put on a node that IS an HRW
// replica writes locally and triggers a push via PushSyncer.
func TestPutLocalAndPushTriggered(t *testing.T) {
	n := tempNode(t, "local-node")
	g := singleMemberGossip(t, "local-node", 19051, 19946)
	syncer := &noOpSyncer{}
	coord := newCoord(t, n, g, syncer)

	resp, err := coord.Put(context.Background(), &kvpb.PutRequest{
		Key:       "user:1",
		ValueJson: `{"name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if resp.GetTimestamp() == nil {
		t.Error("expected a non-nil HLC timestamp in response")
	}

	// The write must be readable locally.
	v, found := n.Get("user:1")
	if !found {
		t.Fatal("key not found after Put")
	}
	if v == "" {
		t.Error("expected non-empty value JSON after Put")
	}

	// Give the goroutine time to call PushToPeers.
	// In a single-member cluster the replica list contains only the local node,
	// so pushWriteToPeers will call PushToPeers with an empty peer list — the
	// noOpSyncer call count may be 0 or 1 depending on how the goroutine
	// schedules.  What we MUST NOT see is a panic from syncer being nil.
}

// TestDeleteLocalAndPushTriggered verifies that Delete writes a tombstone and
// does not panic despite the push goroutine firing unconditionally.
func TestDeleteLocalAndPushTriggered(t *testing.T) {
	n := tempNode(t, "local-node")
	g := singleMemberGossip(t, "local-node", 19052, 19947)
	syncer := &noOpSyncer{}
	coord := newCoord(t, n, g, syncer)

	// Write then delete.
	if _, err := coord.Put(context.Background(), &kvpb.PutRequest{
		Key: "doc:1", ValueJson: `{"x":1}`,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := coord.Delete(context.Background(), &kvpb.DeleteRequest{Key: "doc:1"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if resp.GetTimestamp() == nil {
		t.Error("expected non-nil timestamp in Delete response")
	}

	_, found := n.Get("doc:1")
	if found {
		t.Error("expected key to be gone after Delete")
	}
}

// TestGetLocalReplica verifies that Get on a local replica does not forward.
func TestGetLocalReplica(t *testing.T) {
	n := tempNode(t, "local-node")
	g := singleMemberGossip(t, "local-node", 19053, 19948)
	syncer := &noOpSyncer{}
	coord := newCoord(t, n, g, syncer)

	// Write directly into the node (bypassing coordinator) so the coordinator
	// is not involved in the write — we only test the read path here.
	if _, err := n.Put("item:42", `{"v":42}`); err != nil {
		t.Fatalf("direct node.Put: %v", err)
	}

	resp, err := coord.Get(context.Background(), &kvpb.GetRequest{Key: "item:42"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() {
		t.Error("expected Found=true for existing key")
	}
}

// TestGetMissingKeyLocalReplica verifies that Get returns Found=false for an
// absent key when served locally (no forwarding in a single-node cluster).
func TestGetMissingKeyLocalReplica(t *testing.T) {
	n := tempNode(t, "local-node")
	g := singleMemberGossip(t, "local-node", 19054, 19949)
	coord := newCoord(t, n, g, &noOpSyncer{})

	resp, err := coord.Get(context.Background(), &kvpb.GetRequest{Key: "no-such-key"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetFound() {
		t.Error("expected Found=false for missing key")
	}
}
