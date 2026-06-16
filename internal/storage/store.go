// Package storage wraps Pebble with the key layout of spec section 3.4
// (amended: the merkle bucket sits inside the document key, so bucket scans
// are bounded range scans instead of partition scans with a filter):
//
//	'd' ‖ partitionID(uint16 BE) ‖ bucket(uint16 BE) ‖ userKey -> canonical Document
//	'm' ‖ partitionID ‖ treePath                               -> merkle node hash
//	'g' ‖ partitionID ‖ bucket ‖ userKey                       -> GC clean-round counter
//	'x' ‖ name                                                 -> node meta
//
// All related writes (document + merkle leaf) go through one synced atomic
// batch: a crash never tears a document apart from its merkle leaf, and the
// document's own persisted causal context is the dot-minting checkpoint.
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/nodeid"
)

const (
	prefixDoc    = 'd'
	prefixMerkle = 'm'
	prefixGC     = 'g'
	prefixMeta   = 'x'
)

const (
	metaNodeID     = "node_id"
	metaPartitions = "partitions"
	metaHLC        = "hlc"
	metaOwned      = "owned"
	metaLastAlive  = "last_alive"
)

type Store struct {
	db *pebble.DB
}

func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: slogAdapter{}})
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", dir, err)
	}
	return &Store{db: db}, nil
}

// slogAdapter routes pebble's internal logging to slog at debug level.
type slogAdapter struct{}

func (slogAdapter) Infof(format string, args ...any) {
	slog.Debug("pebble: " + fmt.Sprintf(format, args...))
}

func (slogAdapter) Errorf(format string, args ...any) {
	slog.Error("pebble: " + fmt.Sprintf(format, args...))
}

func (slogAdapter) Fatalf(format string, args ...any) {
	slog.Error("pebble fatal: " + fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (s *Store) Close() error { return s.db.Close() }

// --- keys -------------------------------------------------------------------

func partitionKey(prefix byte, pid uint16, rest []byte) []byte {
	k := make([]byte, 0, 3+len(rest))
	k = append(k, prefix)
	k = binary.BigEndian.AppendUint16(k, pid)
	return append(k, rest...)
}

// bucketKey builds keys for the prefixes that embed the merkle bucket
// ('d' and 'g'): prefix ‖ pid ‖ bucket ‖ userKey. Putting the bucket in the
// key makes one bucket a contiguous range, so anti-entropy bucket scans are
// O(bucket), not O(partition).
func bucketKey(prefix byte, pid uint16, key []byte) []byte {
	k := make([]byte, 0, 5+len(key))
	k = append(k, prefix)
	k = binary.BigEndian.AppendUint16(k, pid)
	k = binary.BigEndian.AppendUint16(k, merkle.Bucket(key))
	return append(k, key...)
}

// userKeyOffset is where the user key starts in a bucketKey.
const userKeyOffset = 5

func metaKey(name string) []byte {
	return append([]byte{prefixMeta}, name...)
}

// --- documents ----------------------------------------------------------------

// GetDocument returns the stored document for (pid, key), or nil if absent.
func (s *Store) GetDocument(pid uint16, key []byte) (*crdt.Document, error) {
	v, closer, err := s.db.Get(bucketKey(prefixDoc, pid, key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	doc, err := crdt.DecodeDocument(v)
	if err != nil {
		return nil, fmt.Errorf("storage: corrupt document %q in partition %d: %w", key, pid, err)
	}
	return doc, nil
}

// ScanPartition calls fn for every document of a partition, in (bucket,
// key) order, over a consistent snapshot.
func (s *Store) ScanPartition(pid uint16, fn func(key []byte, doc *crdt.Document) error) error {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: partitionKey(prefixDoc, pid, nil),
		UpperBound: partitionUpperBound(prefixDoc, pid),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	return scanDocs(iter, pid, fn)
}

// ScanBucket calls fn for every document of one merkle bucket, in key order,
// over a consistent snapshot — a bounded range scan thanks to the bucket
// being part of the document key.
func (s *Store) ScanBucket(pid, bucket uint16, fn func(key []byte, doc *crdt.Document) error) error {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: bucketLowerBound(prefixDoc, pid, bucket),
		UpperBound: bucketUpperBound(prefixDoc, pid, bucket),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	return scanDocs(iter, pid, fn)
}

func scanDocs(iter *pebble.Iterator, pid uint16, fn func(key []byte, doc *crdt.Document) error) error {
	for iter.First(); iter.Valid(); iter.Next() {
		doc, err := crdt.DecodeDocument(iter.Value())
		if err != nil {
			return fmt.Errorf("storage: corrupt document %q in partition %d: %w",
				iter.Key()[userKeyOffset:], pid, err)
		}
		userKey := make([]byte, len(iter.Key())-userKeyOffset)
		copy(userKey, iter.Key()[userKeyOffset:])
		if err := fn(userKey, doc); err != nil {
			return err
		}
	}
	return iter.Error()
}

// bucketLowerBound is the inclusive lower bound of one bucket's keyspace.
func bucketLowerBound(prefix byte, pid, bucket uint16) []byte {
	k := []byte{prefix}
	k = binary.BigEndian.AppendUint16(k, pid)
	return binary.BigEndian.AppendUint16(k, bucket)
}

// bucketUpperBound is the exclusive upper bound of one bucket's keyspace.
func bucketUpperBound(prefix byte, pid, bucket uint16) []byte {
	if bucket == 0xFFFF {
		return partitionUpperBound(prefix, pid)
	}
	return bucketLowerBound(prefix, pid, bucket+1)
}

// partitionUpperBound is the exclusive upper bound of one partition's keyspace.
func partitionUpperBound(prefix byte, pid uint16) []byte {
	if pid == 0xFFFF {
		return []byte{prefix + 1}
	}
	return partitionKey(prefix, pid+1, nil)
}

// GCCounter returns the clean-round counter for one key (0 if absent).
func (s *Store) GCCounter(pid uint16, key []byte) (uint32, error) {
	v, closer, err := s.db.Get(bucketKey(prefixGC, pid, key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()
	if len(v) != 4 {
		return 0, fmt.Errorf("storage: gc counter has %d bytes", len(v))
	}
	return binary.BigEndian.Uint32(v), nil
}

// PersistLastAlive records a liveness lease heartbeat. A node restarting
// with an expired lease (older than the crash grace period) must wipe its
// data: the cluster may have garbage-collected tombstone state it missed,
// and serving its stale documents would resurrect deleted data.
func (s *Store) PersistLastAlive(t time.Time) error {
	return s.db.Set(metaKey(metaLastAlive),
		binary.BigEndian.AppendUint64(nil, uint64(t.UnixMilli())), pebble.NoSync)
}

// LastAlive returns the last liveness heartbeat (zero time if never written).
func (s *Store) LastAlive() (time.Time, error) {
	ms, err := s.metaUint64(metaLastAlive)
	if err != nil || ms == 0 {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(ms)), nil
}

// WipeData deletes all documents, merkle leaves, GC counters, and the
// ownership bitmap — node identity, partition count, and the HLC checkpoint
// survive. Dots are minted per document from its persisted context, so a
// wipe MUST be paired with identity rotation: the wiped contexts were the
// only record of what this actor had minted.
func (s *Store) WipeData() error {
	b := s.db.NewBatch()
	for _, prefix := range []byte{prefixDoc, prefixMerkle, prefixGC} {
		if err := b.DeleteRange([]byte{prefix}, []byte{prefix + 1}, nil); err != nil {
			_ = b.Close()
			return err
		}
	}
	if err := b.Delete(metaKey(metaOwned), nil); err != nil {
		_ = b.Close()
		return err
	}
	return b.Commit(pebble.Sync)
}

// ClearGCCounters deletes every GC clean-round counter of one partition
// (certification progress is voided by a dirty anti-entropy round).
func (s *Store) ClearGCCounters(pid uint16) error {
	return s.db.DeleteRange(partitionKey(prefixGC, pid, nil), partitionUpperBound(prefixGC, pid), pebble.Sync)
}

// DropPartition deletes one partition's documents, merkle nodes, and GC
// counters in a single synced batch. Used when ownership is lost: the data
// lives on the new owners, and a stale copy kept here could resurrect
// documents they have since deleted and garbage-collected.
func (s *Store) DropPartition(pid uint16) error {
	b := s.db.NewBatch()
	for _, prefix := range []byte{prefixDoc, prefixMerkle, prefixGC} {
		if err := b.DeleteRange(partitionKey(prefix, pid, nil), partitionUpperBound(prefix, pid), nil); err != nil {
			_ = b.Close()
			return err
		}
	}
	return b.Commit(pebble.Sync)
}

// HasPartitionData reports whether any document exists in the partition.
func (s *Store) HasPartitionData(pid uint16) (bool, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: partitionKey(prefixDoc, pid, nil),
		UpperBound: partitionUpperBound(prefixDoc, pid),
	})
	if err != nil {
		return false, err
	}
	defer func() { _ = iter.Close() }()
	return iter.First(), iter.Error()
}

// --- merkle leaves ---------------------------------------------------------------

// MerkleLeaf returns one leaf hash (zero hash if never written).
func (s *Store) MerkleLeaf(pid uint16, bucket uint16) (merkle.Hash, error) {
	var out merkle.Hash
	v, closer, err := s.db.Get(partitionKey(prefixMerkle, pid, merkle.BucketPath(bucket)))
	if errors.Is(err, pebble.ErrNotFound) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer func() { _ = closer.Close() }()
	if len(v) != merkle.HashSize {
		return out, fmt.Errorf("storage: merkle leaf %d/%d has %d bytes", pid, bucket, len(v))
	}
	copy(out[:], v)
	return out, nil
}

// MerkleLeaves loads the partition's whole leaf vector.
func (s *Store) MerkleLeaves(pid uint16) (*[merkle.Buckets]merkle.Hash, error) {
	leaves := new([merkle.Buckets]merkle.Hash)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: partitionKey(prefixMerkle, pid, nil),
		UpperBound: partitionUpperBound(prefixMerkle, pid),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) != 5 || len(iter.Value()) != merkle.HashSize {
			return nil, fmt.Errorf("storage: malformed merkle node %x", k)
		}
		bucket := binary.BigEndian.Uint16(k[3:])
		copy(leaves[bucket][:], iter.Value())
	}
	return leaves, iter.Error()
}

// --- atomic batches -----------------------------------------------------------

type Batch struct {
	b   *pebble.Batch
	err error
}

func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch()} }

func (b *Batch) set(key, val []byte) {
	if b.err == nil {
		b.err = b.b.Set(key, val, nil)
	}
}

func (b *Batch) SetDocument(pid uint16, key []byte, doc *crdt.Document) {
	b.set(bucketKey(prefixDoc, pid, key), doc.Canonical())
}

// SetDocumentRaw writes a document already serialized to its canonical bytes.
// Callers that have just computed Canonical() (e.g. for the merkle leaf hash)
// use this to avoid serializing the same document a second time.
func (b *Batch) SetDocumentRaw(pid uint16, key, canonical []byte) {
	b.set(bucketKey(prefixDoc, pid, key), canonical)
}

// DeleteDocument removes a document outright (GC of residual contexts only —
// normal deletes write a residual document instead).
func (b *Batch) DeleteDocument(pid uint16, key []byte) {
	if b.err == nil {
		b.err = b.b.Delete(bucketKey(prefixDoc, pid, key), nil)
	}
}

func (b *Batch) SetMerkleNode(pid uint16, path, hash []byte) {
	b.set(partitionKey(prefixMerkle, pid, path), hash)
}

func (b *Batch) SetGCCounter(pid uint16, key []byte, rounds uint32) {
	b.set(bucketKey(prefixGC, pid, key), binary.BigEndian.AppendUint32(nil, rounds))
}

func (b *Batch) DeleteGCCounter(pid uint16, key []byte) {
	if b.err == nil {
		b.err = b.b.Delete(bucketKey(prefixGC, pid, key), nil)
	}
}

// Commit applies the batch atomically and synced to disk.
func (s *Store) Commit(b *Batch) error {
	if b.err != nil {
		_ = b.b.Close()
		return b.err
	}
	return b.b.Commit(pebble.Sync)
}

// --- meta -----------------------------------------------------------------------

// PersistHLC durably checkpoints the hybrid logical clock.
func (s *Store) PersistHLC(ts uint64) error {
	return s.db.Set(metaKey(metaHLC), binary.BigEndian.AppendUint64(nil, ts), pebble.Sync)
}

// PersistOwned durably records the set of owned partitions (a bitmap), so a
// restart within the crash grace period can resume ownership without a
// bootstrap transfer.
func (s *Store) PersistOwned(bitmap []byte) error {
	return s.db.Set(metaKey(metaOwned), bitmap, pebble.Sync)
}

// Owned returns the persisted ownership bitmap (nil if never written).
func (s *Store) Owned() ([]byte, error) {
	v, closer, err := s.db.Get(metaKey(metaOwned))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// HLC returns the persisted HLC checkpoint (0 if never written).
func (s *Store) HLC() (uint64, error) { return s.metaUint64(metaHLC) }

func (s *Store) metaUint64(name string) (uint64, error) {
	v, closer, err := s.db.Get(metaKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()
	if len(v) != 8 {
		return 0, fmt.Errorf("storage: meta %s has %d bytes, want 8", name, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// BindNodeID pins the owning node's identity to this data directory on first
// use and rejects any other identity afterwards (protects against pointing a
// node at a foreign data dir, which would forge that actor's dots).
func (s *Store) BindNodeID(id nodeid.ID) error {
	return s.bindMeta(metaNodeID, id[:], "node id")
}

// RebindNodeID overwrites the pinned identity — only valid together with a
// data wipe and identity rotation (the fresh actor has minted nothing).
func (s *Store) RebindNodeID(id nodeid.ID) error {
	return s.db.Set(metaKey(metaNodeID), id[:], pebble.Sync)
}

// BindPartitionCount pins the cluster partition count P at bootstrap.
func (s *Store) BindPartitionCount(p uint16) error {
	return s.bindMeta(metaPartitions, binary.BigEndian.AppendUint16(nil, p), "partition count")
}

func (s *Store) bindMeta(name string, want []byte, what string) error {
	v, closer, err := s.db.Get(metaKey(name))
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		return s.db.Set(metaKey(name), want, pebble.Sync)
	case err != nil:
		return err
	}
	defer func() { _ = closer.Close() }()
	if !bytes.Equal(v, want) {
		return fmt.Errorf("storage: data dir belongs to a different %s (stored %x, ours %x)", what, v, want)
	}
	return nil
}
