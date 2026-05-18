package syncer_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
	"github.com/janthoXO/convergeKV/internal/syncer"
)

const IBLT_DEFAULT_CELLS = 512

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

// countRecords counts (key, field) pairs in a node's store via IterateAll.
func countRecords(t *testing.T, n *node.Node) int {
	t.Helper()
	count := 0
	if err := n.Store().IterateAll(func(_, _ string, _ crdt.FieldEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("IterateAll: %v", err)
	}
	return count
}

// TestIBLTStateRoundTrip verifies that the IBLTState serialisation is
// deterministic and that an IBLT built from the store matches the live one.
func TestIBLTStateRoundTrip(t *testing.T) {
	n := tempNode(t, "test-node")
	is := syncer.NewIBLTState(IBLT_DEFAULT_CELLS)
	n.SetIBLTState(is)

	// Write some data.
	for i := 0; i < 20; i++ {
		v := fmt.Sprintf(`{"x":%d}`, i)
		if _, _, err := n.Put(fmt.Sprintf("key-%d", i), v); err != nil {
			t.Fatal(err)
		}
	}

	// Build a second IBLTState directly from the store and compare.
	is2, err := syncer.BuildFromStore(n.Store(), IBLT_DEFAULT_CELLS)
	if err != nil {
		t.Fatalf("BuildFromStore: %v", err)
	}

	// The two IBLTs should have identical symmetric difference = empty.
	diff := is.Snapshot().SubtractUnsafe(is2.Snapshot())
	onlyA, onlyB, ok := diff.Decode()
	if !ok {
		t.Fatal("diff decode failed — IBLTs diverged")
	}
	if len(onlyA) != 0 || len(onlyB) != 0 {
		t.Errorf("IBLTs differ: onlyA=%d onlyB=%d", len(onlyA), len(onlyB))
	}
}

// TestDeserialiseItemRoundTrip verifies the binary serialisation is invertible.
func TestDeserialiseItemRoundTrip(t *testing.T) {
	ts := hlc.Timestamp{PhysicalMs: 1234567890, Logical: 42}
	entry := crdt.FieldEntry{
		Value:     json.RawMessage(`"hello"`),
		Timestamp: ts,
		ReplicaID: "replica-abc",
		Deleted:   false,
	}
	key, field := "my-key", "my-field"

	is := syncer.NewIBLTState(IBLT_DEFAULT_CELLS)
	is.InsertEntry(key, field, entry)

	// Export the item bytes and verify deserialisation.
	// We need to access the serialised form; we'll use the exported DeserialiseItem.
	// Build item bytes manually.
	snap := is.Snapshot()
	// We inserted 1 item; subtract empty to get the diff = the one item.
	diff := snap.SubtractUnsafe(iblt.New(IBLT_DEFAULT_CELLS))
	onlyA, _, ok := diff.Decode()
	if !ok {
		t.Fatal("decode failed")
	}
	if len(onlyA) != 1 {
		t.Fatalf("expected 1 item, got %d", len(onlyA))
	}

	k, f, rID, physMs, logical, deleted, valid := syncer.DeserialiseItem(onlyA[0])
	if !valid {
		t.Fatal("DeserialiseItem returned invalid")
	}
	if k != key || f != field || rID != "replica-abc" ||
		physMs != ts.PhysicalMs || logical != ts.Logical || deleted {
		t.Errorf("deserialised mismatch: k=%s f=%s rID=%s physMs=%d logical=%d deleted=%v",
			k, f, rID, physMs, logical, deleted)
	}
}

// TestIBLTConvergence simulates two nodes diverging and then reconciling
// through IBLT sync without an actual gRPC server.
// This tests the core invariant: symmetric difference is correctly identified.
func TestIBLTConvergence(t *testing.T) {
	n1 := tempNode(t, "n1")
	n2 := tempNode(t, "n2")
	is1 := syncer.NewIBLTState(IBLT_DEFAULT_CELLS)
	is2 := syncer.NewIBLTState(IBLT_DEFAULT_CELLS)
	n1.SetIBLTState(is1)
	n2.SetIBLTState(is2)

	// Write 5 keys to n1 only.
	for i := 0; i < 5; i++ {
		if _, _, err := n1.Put(fmt.Sprintf("key-n1-%d", i), `{"v":1}`); err != nil {
			t.Fatal(err)
		}
	}
	// Write 5 different keys to n2 only.
	for i := 0; i < 5; i++ {
		if _, _, err := n2.Put(fmt.Sprintf("key-n2-%d", i), `{"v":2}`); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate IBLT exchange: n1 sends its IBLT to n2.
	snap1 := is1.Snapshot()
	snap2 := is2.Snapshot()

	diff := snap2.SubtractUnsafe(snap1) // from n2's perspective
	onlyInN2, onlyInN1, ok := diff.Decode()
	if !ok {
		t.Fatal("IBLT decode failed")
	}

	// onlyInN2: 5 items n2 has that n1 doesn't (key-n2-*)
	// onlyInN1: 5 items n1 has that n2 doesn't (key-n1-*)
	if len(onlyInN2) != 5 {
		t.Errorf("expected 5 items in onlyInN2, got %d", len(onlyInN2))
	}
	if len(onlyInN1) != 5 {
		t.Errorf("expected 5 items in onlyInN1, got %d", len(onlyInN1))
	}

	// Now sync: n1 applies n2's items, n2 applies n1's items.
	// Use GetField instead of iterating a full snapshot.
	for _, itemBytes := range onlyInN2 {
		k, f, rID, physMs, logical, deleted, valid := syncer.DeserialiseItem(itemBytes)
		if !valid {
			t.Fatal("invalid item bytes")
		}
		entry, found, err := n2.Store().GetField(k, f)
		if err != nil {
			t.Fatalf("GetField n2 %s/%s: %v", k, f, err)
		}
		if found &&
			entry.Timestamp.PhysicalMs == physMs &&
			entry.Timestamp.Logical == logical &&
			entry.ReplicaID == rID &&
			entry.Deleted == deleted {
			if _, err := n1.ApplyDelta(k, f, entry); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, itemBytes := range onlyInN1 {
		k, f, rID, physMs, logical, deleted, valid := syncer.DeserialiseItem(itemBytes)
		if !valid {
			t.Fatal("invalid item bytes")
		}
		entry, found, err := n1.Store().GetField(k, f)
		if err != nil {
			t.Fatalf("GetField n1 %s/%s: %v", k, f, err)
		}
		if found &&
			entry.Timestamp.PhysicalMs == physMs &&
			entry.Timestamp.Logical == logical &&
			entry.ReplicaID == rID &&
			entry.Deleted == deleted {
			if _, err := n2.ApplyDelta(k, f, entry); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Both nodes should now have 10 keys.
	count1 := countRecords(t, n1)
	count2 := countRecords(t, n2)
	if count1 != 10 {
		t.Errorf("n1: expected 10 records after sync, got %d", count1)
	}
	if count2 != 10 {
		t.Errorf("n2: expected 10 records after sync, got %d", count2)
	}
}
