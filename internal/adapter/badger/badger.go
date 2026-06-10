// Package badger provides a BadgerDB-backed implementation of ports.Store.
// Storage key encoding: partitionID(4B big-endian) + key + "\x00" + field.
package badger

import (
	"bytes"
	"context"
	"encoding/json"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
)

const iterPageSize = 1000

// Store wraps a BadgerDB instance and satisfies ports.Store.
type Store struct {
	db *badgerdb.DB
}

// Open opens (or creates) a BadgerDB database at dir.
func Open(dir string) (*Store, error) {
	opts := badgerdb.DefaultOptions(dir).WithLogger(nil)
	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the BadgerDB handle.
func (s *Store) Close() error { return s.db.Close() }

func badgerKey(partitionId uint32, key, field string) []byte {
	return keyspace.EncodeKey(partitionId, key, field)
}
func partitionPrefix(partitionId uint32) []byte       { return keyspace.PartitionPrefix(partitionId) }
func keyPrefix(partitionId uint32, key string) []byte { return keyspace.KeyPrefix(partitionId, key) }

func decodeKey(b []byte) (uint32, string, string) {
	partitionId, k, f, _ := keyspace.DecodeKey(b)
	return partitionId, k, f
}

// StoredEntry is the on-disk representation of a single FieldEntry.
type StoredEntry struct {
	ValueJSON  []byte `json:"value"`
	PhysicalMs uint64 `json:"phys_ms"`
	Logical    uint32 `json:"logical"`
	ReplicaID  string `json:"replica_id"`
	Deleted    bool   `json:"deleted"`
}

func decodeEntry(v []byte) (crdt.FieldEntry, error) {
	var se StoredEntry
	if err := json.Unmarshal(v, &se); err != nil {
		return crdt.FieldEntry{}, err
	}
	return crdt.NewFieldEntry(se.ValueJSON, se.PhysicalMs, se.Logical, se.ReplicaID, se.Deleted), nil
}

// GetKey reads all fields for a key and returns them as an AWLWWMap.
func (s *Store) GetKey(ctx context.Context, partitionId uint32, key string) (crdt.AWLWWMap, error) {
	if ctx.Err() != nil {
		return crdt.NewAWLWWMap(), ctx.Err()
	}
	m := crdt.NewAWLWWMap()
	prefix := keyPrefix(partitionId, key)
	err := s.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			_, _, field := decodeKey(k)
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}
			m.Fields[field] = entry
		}
		return nil
	})
	return m, err
}

// GetField reads a single (key, field) record.
func (s *Store) GetField(ctx context.Context, partitionId uint32, key, field string) (crdt.FieldEntry, bool, error) {
	if ctx.Err() != nil {
		return crdt.FieldEntry{}, false, ctx.Err()
	}
	var entry crdt.FieldEntry
	var found bool
	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(badgerKey(partitionId, key, field))
		if err == badgerdb.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		entry, err = decodeEntry(v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return entry, found, err
}

type iterPage struct {
	key    string
	field  string
	entry  crdt.FieldEntry
	rawKey []byte
}

func (s *Store) pagedScan(ctx context.Context, prefix []byte, fn func(key, field string, entry crdt.FieldEntry) error) error {
	var cursor []byte
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		page := make([]iterPage, 0, iterPageSize)
		if err := s.db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			if prefix != nil {
				opts.Prefix = prefix
			}
			it := txn.NewIterator(opts)
			defer it.Close()

			if cursor == nil {
				it.Rewind()
			} else {
				it.Seek(cursor)
				if it.Valid() && bytes.Equal(it.Item().Key(), cursor) {
					it.Next()
				}
			}

			for ; it.Valid() && len(page) < iterPageSize; it.Next() {
				item := it.Item()
				k := item.KeyCopy(nil)
				if bytes.Equal(k, keyspace.CheckpointKey()) {
					// Reserved HLC checkpoint record, not a (key, field) entry.
					continue
				}
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				_, key, field := decodeKey(k)
				entry, err := decodeEntry(v)
				if err != nil {
					return err
				}
				page = append(page, iterPage{key: key, field: field, entry: entry, rawKey: k})
			}
			return nil
		}); err != nil {
			return err
		}

		if len(page) == 0 {
			return nil
		}
		for _, rec := range page {
			if err := fn(rec.key, rec.field, rec.entry); err != nil {
				return err
			}
		}
		cursor = page[len(page)-1].rawKey
		if len(page) < iterPageSize {
			return nil
		}
	}
}

// IterateAll streams every (key, field, entry) record.
func (s *Store) IterateAll(ctx context.Context, fn func(key, field string, entry crdt.FieldEntry) error) error {
	return s.pagedScan(ctx, nil, fn)
}

// IteratePartition streams every record for the given partition.
func (s *Store) IteratePartition(ctx context.Context, partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error {
	return s.pagedScan(ctx, partitionPrefix(partitionId), fn)
}

// SaveBatch persists a batch of field entries in a single atomic transaction.
func (s *Store) SaveBatch(ctx context.Context, entries []crdt.FieldUpdate) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.db.Update(func(txn *badgerdb.Txn) error {
		for _, u := range entries {
			se := StoredEntry{
				ValueJSON:  u.Entry.Value,
				PhysicalMs: u.Entry.Timestamp.PhysicalMs,
				Logical:    u.Entry.Timestamp.Logical,
				ReplicaID:  u.Entry.ReplicaID,
				Deleted:    u.Entry.Deleted,
			}
			b, err := json.Marshal(se)
			if err != nil {
				return err
			}
			if err := txn.Set(badgerKey(u.PartitionID, u.Key, u.Field), b); err != nil {
				return err
			}
		}
		return nil
	})
}

// checkpointValue is the on-disk representation of an HLC checkpoint.
type checkpointValue struct {
	PhysicalMs uint64 `json:"phys_ms"`
	Logical    uint32 `json:"logical"`
}

// SaveCheckpoint persists ts under the reserved checkpoint key.
func (s *Store) SaveCheckpoint(ctx context.Context, ts hlc.Timestamp) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	b, err := json.Marshal(checkpointValue{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical})
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Set(keyspace.CheckpointKey(), b)
	})
}

// LoadCheckpoint reads the checkpoint saved by SaveCheckpoint, if any.
func (s *Store) LoadCheckpoint(ctx context.Context) (hlc.Timestamp, bool, error) {
	if ctx.Err() != nil {
		return hlc.Timestamp{}, false, ctx.Err()
	}
	var cv checkpointValue
	found := false
	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(keyspace.CheckpointKey())
		if err == badgerdb.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(v, &cv); err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return hlc.Timestamp{}, false, err
	}
	return hlc.Timestamp{PhysicalMs: cv.PhysicalMs, Logical: cv.Logical}, found, nil
}
