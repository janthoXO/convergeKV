package clustertest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/placement"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

func TestAERepairsLaggedReplica(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()
	client := h.Client(0)

	// Write a handful of keys, wait for normal replication.
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("ae-key-%d", i)
		doc := fmt.Sprintf(`{"i": %d}`, i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: keys[i], Value: []byte(doc)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range keys {
		h.WaitOwnersConverged(k, time.Second)
	}

	// Lag one replica: remove a document and rebuild its leaf honestly, so
	// the victim looks exactly like a replica that never saw the deltas
	// (consistent tree, stale data).
	victim := h.Nodes[2]
	pid := placement.Partition([]byte(keys[3]), Partitions)
	b := victim.Store.NewBatch()
	b.DeleteDocument(pid, []byte(keys[3]))
	if err := victim.Store.Commit(b); err != nil {
		t.Fatal(err)
	}
	if err := victim.Coord.RecomputeMerkleLeaf(pid, merkle.Bucket([]byte(keys[3]))); err != nil {
		t.Fatal(err)
	}

	// One AE round on the victim must restore byte-equal state everywhere.
	// (The background engines may beat us to it via the push direction —
	// either way the state must converge.)
	if err := victim.AE.RunRound(ctx, pid); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(keys[3], 2*time.Second)

	// And the restored replica serves the document again.
	doc, err := victim.Store.GetDocument(pid, []byte(keys[3]))
	if err != nil || doc == nil {
		t.Fatalf("victim still missing the doc: %v %v", doc, err)
	}
}

func TestAECleanRoundsAreCheapAndCounted(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()
	client := h.Client(0)

	const key = "clean-key"
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)
	pid := placement.Partition([]byte(key), Partitions)

	nd := h.Nodes[0]
	repairsBefore := nd.AE.KeysRepaired()
	leavesBefore := nd.AE.LeafFetches()

	for i := 0; i < 3; i++ {
		if err := nd.AE.RunRound(ctx, pid); err != nil {
			t.Fatal(err)
		}
	}
	if nd.AE.KeysRepaired() != repairsBefore {
		t.Fatal("steady-state rounds must repair nothing")
	}
	// Clean rounds exchange only roots: no leaf vectors fetched — that is
	// the O(tree) vs O(keys) traffic bound.
	if nd.AE.LeafFetches() != leavesBefore {
		t.Fatal("clean rounds must not fetch leaf vectors")
	}
	if nd.AE.CleanRounds(pid) < 3 {
		t.Fatalf("clean round counter = %d, want >= 3", nd.AE.CleanRounds(pid))
	}
}

func TestAEHealsDroppedDeltas(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()

	// Apply a write directly on one owner's coordinator, with the fan-out
	// silenced by... applying to local state only via MergeDelta on a
	// foreign delta. Simplest equivalent of "all deltas dropped": write via
	// node 0's applier path, then wipe the doc from node 2 again.
	client := h.Client(0)
	const key = "dropped-delta"
	if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": "x"}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, time.Second)

	pid := placement.Partition([]byte(key), Partitions)
	victim := h.Nodes[1]
	b := victim.Store.NewBatch()
	b.DeleteDocument(pid, []byte(key))
	b.SetMerkleNode(pid, merkle.BucketPath(merkle.Bucket([]byte(key))), make([]byte, merkle.HashSize))
	if err := victim.Store.Commit(b); err != nil {
		t.Fatal(err)
	}

	// A round initiated by a HEALTHY peer must also detect and push the fix
	// (repair works in both directions).
	if err := h.Nodes[0].AE.RunRound(ctx, pid); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, 2*time.Second)
}
