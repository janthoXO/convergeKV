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

// decodeEntry unmarshals a stored value blob into a crdt.FieldEntry.
func decodeEntry(v []byte) (crdt.FieldEntry, error) {
	var se StoredEntry
	if err := json.Unmarshal(v, &se); err != nil {
		return crdt.FieldEntry{}, err
	}
	return crdt.FieldEntry{
		Value:     se.ValueJSON,
		Timestamp: hlc.Timestamp{PhysicalMs: se.PhysicalMs, Logical: se.Logical},
		ReplicaID: se.ReplicaID,
		Deleted:   se.Deleted,
	}, nil
}

// GetKey reads all fields for a key from BadgerDB and returns them as an AWLWWMap.
// Returns an empty map (not an error) when the key has no records.
func (s *Store) GetKey(key string) (crdt.AWLWWMap, error) {
	m := crdt.NewAWLWWMap()
	prefix := []byte(key + "\x00")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
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
			_, field := decodeKey(k)
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

// GetField reads a single (key, field) record from BadgerDB.
// Returns (entry, true, nil) if found, (zero, false, nil) if absent.
func (s *Store) GetField(key, field string) (crdt.FieldEntry, bool, error) {
	var entry crdt.FieldEntry
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(badgerKey(key, field))
		if err == badger.ErrKeyNotFound {
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

// MaxTimestamp scans all persisted entries and returns the highest HLC timestamp
// found. Returns a zero timestamp when the store is empty.
// Used at startup to seed the HLC above any value written before a crash so
// a backwards NTP correction cannot break LWW causality.
func (s *Store) MaxTimestamp() (hlc.Timestamp, error) {
	var floor hlc.Timestamp
	err := s.IterateAll(func(_, _ string, e crdt.FieldEntry) error {
		if hlc.Less(floor, e.Timestamp) {
			floor = e.Timestamp
		}
		return nil
	})
	return floor, err
}

// IterateAll streams every (key, field, entry) record from BadgerDB, calling fn
// for each. Iteration stops early and returns fn's error if fn returns non-nil.
func (s *Store) IterateAll(fn func(key, field string, entry crdt.FieldEntry) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			key, field := decodeKey(k)
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}
			if err := fn(key, field, entry); err != nil {
				return err
			}
		}
		return nil
	})
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
