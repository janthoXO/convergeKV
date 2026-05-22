// Package storage provides a BadgerDB-backed persistence layer for CRDT state.
// Each (key, field) pair is stored as a separate BadgerDB record, enabling
// efficient iteration and partial updates during replication.
//
// Storage key encoding: partitionID(4B big-endian) + key + "\x00" + field.
// The 4-byte partition prefix makes every partition a contiguous range that
// can be scanned with a single prefix seek. NUM_PARTITIONS must be identical
// on every cluster node — changing it after data is written requires a fresh
// data directory.
package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
)

const iterPageSize = 1000

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

// badgerKey encodes (partitionID, key, field) into a storage key:
// 4B big-endian partition prefix + key + "\x00" + field.
func badgerKey(partitionId uint32, key, field string) []byte {
	kb := []byte(key)
	fb := []byte(field)
	buf := make([]byte, 4+len(kb)+1+len(fb))
	binary.BigEndian.PutUint32(buf, partitionId)
	copy(buf[4:], kb)
	buf[4+len(kb)] = 0x00
	copy(buf[4+len(kb)+1:], fb)
	return buf
}

// partitionPrefix returns the 4-byte prefix used to scan all entries for partitionId.
func partitionPrefix(partitionId uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, partitionId)
	return b
}

// keyPrefix returns the prefix for a GetKey scan: partitionId(4B) + key + "\x00".
func keyPrefix(partitionId uint32, key string) []byte {
	kb := []byte(key)
	buf := make([]byte, 4+len(kb)+1)
	binary.BigEndian.PutUint32(buf, partitionId)
	copy(buf[4:], kb)
	buf[4+len(kb)] = 0x00
	return buf
}

// decodeKey splits a storage key back into (partitionID, key, field).
// The first 4 bytes are the big-endian partition ID; the remainder is split on
// the first "\x00" to separate the key from the field (fields may contain
// "\x00" and round-trip correctly because we split on the first occurrence).
func decodeKey(b []byte) (partitionId uint32, key, field string) {
	if len(b) < 4 {
		return 0, string(b), ""
	}
	partitionId = binary.BigEndian.Uint32(b[:4])
	rest := string(b[4:])
	parts := strings.SplitN(rest, "\x00", 2)
	if len(parts) != 2 {
		return partitionId, rest, ""
	}
	return partitionId, parts[0], parts[1]
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
func (s *Store) GetKey(partitionId uint32, key string) (crdt.AWLWWMap, error) {
	m := crdt.NewAWLWWMap()
	prefix := keyPrefix(partitionId, key)

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

// GetField reads a single (key, field) record from BadgerDB.
// Returns (entry, true, nil) if found, (zero, false, nil) if absent.
func (s *Store) GetField(partitionId uint32, key, field string) (crdt.FieldEntry, bool, error) {
	var entry crdt.FieldEntry
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(badgerKey(partitionId, key, field))
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

// iterPage holds one decoded record collected within a single Badger transaction.
type iterPage struct {
	key    string
	field  string
	entry  crdt.FieldEntry
	rawKey []byte // raw Badger key used as cursor for the next page
}

// pagedScan is the shared paged-scan engine used by IterateAll and IteratePartition.
// prefix, when non-nil, restricts the scan to keys that start with that prefix.
func (s *Store) pagedScan(prefix []byte, fn func(key, field string, entry crdt.FieldEntry) error) error {
	var cursor []byte

	for {
		page := make([]iterPage, 0, iterPageSize)

		if err := s.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
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

// IterateAll streams every (key, field, entry) record from BadgerDB, calling fn
// for each. Iteration stops early and returns fn's error if fn returns non-nil.
//
// Internally the scan is split into pages of iterPageSize records. Each page
// opens its own short-lived read transaction, so Badger's MVCC GC is not
// blocked for the duration of a slow network send.
func (s *Store) IterateAll(fn func(key, field string, entry crdt.FieldEntry) error) error {
	return s.pagedScan(nil, fn)
}

// IteratePartition streams every (key, field, entry) record for the given
// partition, calling fn for each. Uses a prefix seek so only the requested
// partition's contiguous range is read.
func (s *Store) IteratePartition(partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error {
	return s.pagedScan(partitionPrefix(partitionId), fn)
}

// SaveBatch persists a batch of field entries in a single atomic transaction.
func (s *Store) SaveBatch(entries []FieldUpdate) error {
	return s.db.Update(func(txn *badger.Txn) error {
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
