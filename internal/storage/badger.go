// Package storage provides a BadgerDB-backed persistence layer for CRDT state.
// Each (key, field) pair is stored as a separate BadgerDB record, enabling
// efficient iteration and partial updates during replication.
package storage

import (
	"encoding/json"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
)

// Store wraps a BadgerDB instance with CRDT-aware read/write helpers.
type Store struct {
	db *badger.DB
}

// Open opens (or creates) a BadgerDB database at dir.
func Open(dir string) (*Store, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

// Close releases the BadgerDB handle.
func (s *Store) Close() error { return s.db.Close() }

// badgerKey encodes a (key, field) pair into a storage key.
func badgerKey(key, field string) []byte {
	return []byte(key + "\x00" + field)
}

// decodeKey splits a storage key back into (key, field).
func decodeKey(b []byte) (key, field string) {
	parts := strings.SplitN(string(b), "\x00", 2)
	if len(parts) != 2 {
		return string(b), ""
	}

	return parts[0], parts[1]
}

// SaveField persists a single FieldEntry.
func (s *Store) SaveField(key, field string, entry crdt.FieldEntry) error {
	se := StoredEntry{
		ValueJSON:  entry.Value,
		PhysicalMs: entry.Timestamp.PhysicalMs,
		Logical:    entry.Timestamp.Logical,
		ReplicaID:  entry.ReplicaID,
		Deleted:    entry.Deleted,
	}

	b, err := json.Marshal(se)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(badgerKey(key, field), b)
	})
}

// LoadAll reads every (key, field) record from BadgerDB and returns the full
// in-memory state as map[key]AWLWWMap. Called once at node startup.
func (s *Store) LoadAll() (map[string]crdt.AWLWWMap, error) {
	result := make(map[string]crdt.AWLWWMap)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var se StoredEntry
			if err := json.Unmarshal(v, &se); err != nil {
				return err
			}

			mapKey, field := decodeKey(k)
			m, ok := result[mapKey]
			if !ok {
				m = crdt.NewAWLWWMap()
			}

			m.Fields[field] = crdt.FieldEntry{
				Value:     se.ValueJSON,
				Timestamp: hlc.Timestamp{PhysicalMs: se.PhysicalMs, Logical: se.Logical},
				ReplicaID: se.ReplicaID,
				Deleted:   se.Deleted,
			}
			result[mapKey] = m
		}

		return nil
	})

	return result, err
}

// SaveBatch persists a batch of field entries atomically (used during replication).
// Accepts a slice of (key, field, FieldEntry) tuples.
func (s *Store) SaveBatch(entries []FieldUpdate) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

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

		if err := wb.Set(badgerKey(u.Key, u.Field), b); err != nil {
			return err
		}
	}

	return wb.Flush()
}
