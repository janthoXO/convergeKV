// Package storage wraps Pebble with the key layout of spec section 3.4:
//
//	'd' ‖ partitionID(uint16 BE) ‖ userKey  -> canonical Document encoding
//	'm' ‖ partitionID ‖ treePath            -> merkle node hash
//	'g' ‖ partitionID ‖ userKey             -> GC clean-round counter
//	'x' ‖ name                              -> node meta
//
// All related writes (document + merkle leaf + dot-seq checkpoint) go through
// one synced atomic batch: a crash never tears a document apart from its
// merkle leaf and never lets a dot sequence number be reused.
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/janthoXO/convergeKV/internal/crdt"
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
	metaDotSeq     = "dot_seq"
	metaHLC        = "hlc"
)

type Store struct {
	db *pebble.DB
}

func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", dir, err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// --- keys -------------------------------------------------------------------

func partitionKey(prefix byte, pid uint16, rest []byte) []byte {
	k := make([]byte, 0, 3+len(rest))
	k = append(k, prefix)
	k = binary.BigEndian.AppendUint16(k, pid)
	return append(k, rest...)
}

func metaKey(name string) []byte {
	return append([]byte{prefixMeta}, name...)
}

// --- documents ----------------------------------------------------------------

// GetDocument returns the stored document for (pid, key), or nil if absent.
func (s *Store) GetDocument(pid uint16, key []byte) (*crdt.Document, error) {
	v, closer, err := s.db.Get(partitionKey(prefixDoc, pid, key))
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

// ScanPartition calls fn for every document of a partition, in key order,
// over a consistent snapshot.
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

	for iter.First(); iter.Valid(); iter.Next() {
		doc, err := crdt.DecodeDocument(iter.Value())
		if err != nil {
			return fmt.Errorf("storage: corrupt document %q in partition %d: %w", iter.Key()[3:], pid, err)
		}
		userKey := make([]byte, len(iter.Key())-3)
		copy(userKey, iter.Key()[3:])
		if err := fn(userKey, doc); err != nil {
			return err
		}
	}
	return iter.Error()
}

// partitionUpperBound is the exclusive upper bound of one partition's keyspace.
func partitionUpperBound(prefix byte, pid uint16) []byte {
	if pid == 0xFFFF {
		return []byte{prefix + 1}
	}
	return partitionKey(prefix, pid+1, nil)
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
	b.set(partitionKey(prefixDoc, pid, key), doc.Canonical())
}

// DeleteDocument removes a document outright (GC of residual contexts only —
// normal deletes write a residual document instead).
func (b *Batch) DeleteDocument(pid uint16, key []byte) {
	if b.err == nil {
		b.err = b.b.Delete(partitionKey(prefixDoc, pid, key), nil)
	}
}

func (b *Batch) SetMerkleNode(pid uint16, path, hash []byte) {
	b.set(partitionKey(prefixMerkle, pid, path), hash)
}

func (b *Batch) SetGCCounter(pid uint16, key []byte, rounds uint32) {
	b.set(partitionKey(prefixGC, pid, key), binary.BigEndian.AppendUint32(nil, rounds))
}

func (b *Batch) DeleteGCCounter(pid uint16, key []byte) {
	if b.err == nil {
		b.err = b.b.Delete(partitionKey(prefixGC, pid, key), nil)
	}
}

// SetDotSeq checkpoints the actor's last minted dot sequence number. It MUST
// be part of the same batch as every applied local op.
func (b *Batch) SetDotSeq(seq uint64) {
	b.set(metaKey(metaDotSeq), binary.BigEndian.AppendUint64(nil, seq))
}

// SetHLC checkpoints the hybrid logical clock.
func (b *Batch) SetHLC(ts uint64) {
	b.set(metaKey(metaHLC), binary.BigEndian.AppendUint64(nil, ts))
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

// DotSeq returns the persisted dot sequence checkpoint (0 if never written).
func (s *Store) DotSeq() (uint64, error) { return s.metaUint64(metaDotSeq) }

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
func (s *Store) BindNodeID(id [16]byte) error {
	return s.bindMeta(metaNodeID, id[:], "node id")
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
