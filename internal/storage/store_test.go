package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/merkle"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func putDoc(t *testing.T, s *Store, pid uint16, key string, doc *crdt.Document) {
	t.Helper()
	b := s.NewBatch()
	b.SetDocument(pid, []byte(key), doc)
	if err := s.Commit(b); err != nil {
		t.Fatal(err)
	}
}

func TestDocumentRoundTrip(t *testing.T) {
	s := openTest(t)
	m := &crdt.Minter{Actor: crdt.ActorID{1}}
	doc := crdt.NewDocument()
	doc.Put("f", []byte(`"v"`), m.Next(), 100)
	putDoc(t, s, 42, "key", doc)

	got, err := s.GetDocument(42, []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Canonical(), doc.Canonical()) {
		t.Fatal("document round trip mismatch")
	}

	if missing, err := s.GetDocument(42, []byte("other")); err != nil || missing != nil {
		t.Fatalf("absent key: got %v, %v", missing, err)
	}
	if missing, err := s.GetDocument(43, []byte("key")); err != nil || missing != nil {
		t.Fatalf("same key, other partition must be absent: got %v, %v", missing, err)
	}
}

func TestScanPartitionExactness(t *testing.T) {
	s := openTest(t)
	m := &crdt.Minter{Actor: crdt.ActorID{1}}
	mk := func() *crdt.Document {
		d := crdt.NewDocument()
		d.Put("f", []byte(`1`), m.Next(), 100)
		return d
	}
	// Neighbours on both sides, including the partition-ID boundary bytes.
	for _, pid := range []uint16{0, 6, 7, 8, 0xFFFF} {
		for i := 0; i < 3; i++ {
			putDoc(t, s, pid, fmt.Sprintf("key-%d", i), mk())
		}
	}

	var keys []string
	err := s.ScanPartition(7, func(key []byte, doc *crdt.Document) error {
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Scan order is (bucket, key), so compare as a set.
	sort.Strings(keys)
	want := []string{"key-0", "key-1", "key-2"}
	if len(keys) != len(want) {
		t.Fatalf("scan returned %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("scan returned %v, want %v", keys, want)
		}
	}

	// Edge partitions must also scan cleanly.
	for _, pid := range []uint16{0, 0xFFFF} {
		n := 0
		if err := s.ScanPartition(pid, func([]byte, *crdt.Document) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("partition %d: scanned %d docs, want 3", pid, n)
		}
	}
}

func TestScanBucket(t *testing.T) {
	s := openTest(t)
	m := &crdt.Minter{Actor: crdt.ActorID{1}}
	byBucket := map[uint16][]string{}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("key-%d", i)
		d := crdt.NewDocument()
		d.Put("f", []byte(`1`), m.Next(), 100)
		putDoc(t, s, 7, key, d)
		b := merkle.Bucket([]byte(key))
		byBucket[b] = append(byBucket[b], key)
	}
	// A neighbouring partition must never leak into the scan.
	other := crdt.NewDocument()
	other.Put("f", []byte(`1`), m.Next(), 100)
	putDoc(t, s, 8, "key-0", other)

	total := 0
	for bucket, want := range byBucket {
		var got []string
		err := s.ScanBucket(7, bucket, func(key []byte, _ *crdt.Document) error {
			got = append(got, string(key))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("bucket %d: got %v, want %v", bucket, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("bucket %d: got %v, want %v", bucket, got, want)
			}
		}
		total += len(got)
	}
	if total != 64 {
		t.Fatalf("bucket scans covered %d docs, want 64", total)
	}
}

func TestMetaCheckpoints(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ts, _ := s.HLC(); ts != 0 {
		t.Fatalf("fresh store HLC = %d", ts)
	}
	if err := s.PersistHLC(12345); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir) // reopen: checkpoints survive
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if ts, _ := s.HLC(); ts != 12345 {
		t.Fatalf("HLC after reopen = %d, want 12345", ts)
	}
}

func TestBindNodeIDAndPartitionCount(t *testing.T) {
	s := openTest(t)
	id := [16]byte{1, 2, 3}
	if err := s.BindNodeID(id); err != nil {
		t.Fatal(err)
	}
	if err := s.BindNodeID(id); err != nil {
		t.Fatalf("re-bind same id: %v", err)
	}
	if err := s.BindNodeID([16]byte{9}); err == nil {
		t.Fatal("foreign node id must be rejected")
	}
	if err := s.BindPartitionCount(256); err != nil {
		t.Fatal(err)
	}
	if err := s.BindPartitionCount(128); err == nil {
		t.Fatal("changed partition count must be rejected")
	}
}

func TestMerkleAndGCKeys(t *testing.T) {
	s := openTest(t)
	b := s.NewBatch()
	b.SetMerkleNode(3, []byte{0x01}, []byte("hash"))
	b.SetGCCounter(3, []byte("key"), 2)
	if err := s.Commit(b); err != nil {
		t.Fatal(err)
	}
	// Merkle/GC keys must not leak into document scans.
	if err := s.ScanPartition(3, func(key []byte, _ *crdt.Document) error {
		return fmt.Errorf("unexpected document %q", key)
	}); err != nil {
		t.Fatal(err)
	}
}
