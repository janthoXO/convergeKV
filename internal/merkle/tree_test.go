package merkle

import (
	"testing"
)

// TestPartitionIndexConsistency verifies PartitionIndex equals RingToken(key) % NumPartitions.
func TestPartitionIndexConsistency(t *testing.T) {
	keys := []string{"hello", "world", "user:1", "key:extra", "key:shared", "foo", "bar"}
	for _, key := range keys {
		want := int(RingToken(key) % NumPartitions)
		got := PartitionIndex(key)
		if got != want {
			t.Errorf("key=%q: PartitionIndex=%d, want RingToken%%NumPartitions=%d", key, got, want)
		}
	}
}

// TestEmptyTreeRootIsZero verifies that a freshly created tree has the zero root.
func TestEmptyTreeRootIsZero(t *testing.T) {
	tree := NewMerkleTree()
	if tree.Root() != emptyHash {
		t.Errorf("expected zero root for empty tree, got %x", tree.Root())
	}
}

// TestUpdateThenRemoveCancels verifies that Update followed by Remove with the
// same arguments returns the root to the zero hash (XOR self-inverse property).
func TestUpdateThenRemoveCancels(t *testing.T) {
	tree := NewMerkleTree()
	tree.Update("key:1", "name", "r1", 1000, 0)
	if tree.Root() == emptyHash {
		t.Fatal("root should be non-zero after Update")
	}
	tree.Remove("key:1", "name", "r1", 1000, 0)
	if tree.Root() != emptyHash {
		t.Errorf("root should be zero after Remove cancels Update, got %x", tree.Root())
	}
}

// TestCommutativity verifies two trees built by inserting the same entries in
// different orders produce the same root.
func TestCommutativity(t *testing.T) {
	treeA := NewMerkleTree()
	treeA.Update("key:1", "name", "r1", 1000, 0)
	treeA.Update("key:2", "city", "r2", 2000, 1)
	treeA.Update("key:3", "age", "r1", 3000, 0)

	treeB := NewMerkleTree()
	treeB.Update("key:3", "age", "r1", 3000, 0)
	treeB.Update("key:1", "name", "r1", 1000, 0)
	treeB.Update("key:2", "city", "r2", 2000, 1)

	if treeA.Root() != treeB.Root() {
		t.Errorf("roots differ: A=%x B=%x", treeA.Root(), treeB.Root())
	}
}

// TestDivergentPartitions verifies that inserting one extra entry on tree B makes
// DivergentPartitions return exactly the partition containing that entry's key.
func TestDivergentPartitions(t *testing.T) {
	sharedKey := "key:shared"
	extraKey := "key:extra"

	treeA := NewMerkleTree()
	treeA.Update(sharedKey, "name", "r1", 1000, 0)

	treeB := NewMerkleTree()
	treeB.Update(sharedKey, "name", "r1", 1000, 0)
	treeB.Update(extraKey, "city", "r2", 2000, 0) // extra entry only in B

	// From A's perspective: which of A's partitions differ from B's?
	divergentFromA := treeA.DivergentPartitions(treeB.AllPartitionHashes())
	expectedPartition := PartitionIndex(extraKey)

	if len(divergentFromA) != 1 {
		t.Fatalf("expected 1 divergent partition, got %d: %v", len(divergentFromA), divergentFromA)
	}
	if divergentFromA[0] != expectedPartition {
		t.Errorf("expected divergent partition %d (for key %q), got %d", expectedPartition, extraKey, divergentFromA[0])
	}
}

// TestAllPartitionHashesLength verifies AllPartitionHashes always returns exactly NumPartitions hashes.
func TestAllPartitionHashesLength(t *testing.T) {
	tree := NewMerkleTree()
	hashes := tree.AllPartitionHashes()
	if len(hashes) != NumPartitions {
		t.Errorf("expected %d partition hashes, got %d", NumPartitions, len(hashes))
	}

	// Also verify after some updates.
	tree.Update("somekey", "field", "r1", 999, 0)
	hashes = tree.AllPartitionHashes()
	if len(hashes) != NumPartitions {
		t.Errorf("expected %d partition hashes after update, got %d", NumPartitions, len(hashes))
	}
}

// TestNoDivergenceWhenEqual verifies that two identical trees report no divergent partitions.
func TestNoDivergenceWhenEqual(t *testing.T) {
	treeA := NewMerkleTree()
	treeA.Update("key:1", "name", "r1", 1000, 0)

	treeB := NewMerkleTree()
	treeB.Update("key:1", "name", "r1", 1000, 0)

	divergent := treeA.DivergentPartitions(treeB.AllPartitionHashes())
	if len(divergent) != 0 {
		t.Errorf("expected no divergent partitions for identical trees, got %v", divergent)
	}
}

// TestPartitionHashNonZeroAfterUpdate verifies PartitionHash returns non-zero
// for a partition that has received an entry.
func TestPartitionHashNonZeroAfterUpdate(t *testing.T) {
	tree := NewMerkleTree()
	key := "user:42"
	tree.Update(key, "name", "r1", 1000, 0)

	p := PartitionIndex(key)
	h := tree.PartitionHash(p)
	if h == emptyHash {
		t.Errorf("PartitionHash(%d) should be non-zero after Update for key %q", p, key)
	}
}
