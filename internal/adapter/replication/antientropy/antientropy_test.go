package antientropy_test

import (
	"fmt"
	"testing"

	"github.com/janthoXO/convergeKV/internal/adapter/badger"
	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

const ibltCells = 512

func tempReplica(t *testing.T, id string) *replica.Replica {
	t.Helper()
	store, err := badger.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store %s: %v", id, err)
	}
	t.Cleanup(func() { store.Close() })
	r := replica.New(id, hlc.New(), store, ibltCells)
	t.Cleanup(r.Close)
	return r
}

func countRecords(t *testing.T, r *replica.Replica) int {
	t.Helper()
	count := 0
	if err := r.IterateAll(t.Context(), func(_, _ string, _ crdt.FieldEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("IterateAll: %v", err)
	}
	return count
}

// TestIBLTConvergence simulates two nodes diverging and reconciling via IBLT
// diff + ApplyDelta — exercising the same path the anti-entropy loop runs.
func TestIBLTConvergence(t *testing.T) {
	n1 := tempReplica(t, "n1")
	n2 := tempReplica(t, "n2")

	pid := uint32(0)
	if err := n1.EnsurePartition(t.Context(), pid); err != nil {
		t.Fatalf("n1 EnsurePartition: %v", err)
	}
	if err := n2.EnsurePartition(t.Context(), pid); err != nil {
		t.Fatalf("n2 EnsurePartition: %v", err)
	}

	for i := range 5 {
		if _, _, err := n1.Put(t.Context(), pid, fmt.Sprintf("key-n1-%d", i), `{"v":1}`); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		if _, _, err := n2.Put(t.Context(), pid, fmt.Sprintf("key-n2-%d", i), `{"v":2}`); err != nil {
			t.Fatal(err)
		}
	}

	snap1 := n1.IBLTSnapshot(0)
	snap2 := n2.IBLTSnapshot(0)

	diff, err := snap2.Subtract(snap1)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	onlyInN2, onlyInN1, ok := diff.Decode()
	if !ok {
		t.Fatal("IBLT decode failed")
	}
	if len(onlyInN2) != 5 {
		t.Errorf("expected 5 in onlyInN2, got %d", len(onlyInN2))
	}
	if len(onlyInN1) != 5 {
		t.Errorf("expected 5 in onlyInN1, got %d", len(onlyInN1))
	}

	for _, itemBytes := range onlyInN2 {
		k, f, rID, physMs, logical, deleted, valid := domiblt.DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := n2.GetField(t.Context(), pid, k, f)
		if err != nil || !found {
			continue
		}
		if entry.Timestamp.PhysicalMs == physMs && entry.Timestamp.Logical == logical &&
			entry.ReplicaID == rID && entry.Deleted == deleted {
			n1.ApplyDelta(t.Context(), pid, k, f, entry) //nolint:errcheck
		}
	}
	for _, itemBytes := range onlyInN1 {
		k, f, rID, physMs, logical, deleted, valid := domiblt.DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := n1.GetField(t.Context(), pid, k, f)
		if err != nil || !found {
			continue
		}
		if entry.Timestamp.PhysicalMs == physMs && entry.Timestamp.Logical == logical &&
			entry.ReplicaID == rID && entry.Deleted == deleted {
			n2.ApplyDelta(t.Context(), pid, k, f, entry) //nolint:errcheck
		}
	}

	if c := countRecords(t, n1); c != 10 {
		t.Errorf("n1: expected 10 records after sync, got %d", c)
	}
	if c := countRecords(t, n2); c != 10 {
		t.Errorf("n2: expected 10 records after sync, got %d", c)
	}
}
