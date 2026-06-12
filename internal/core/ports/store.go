// Package ports defines the consumer-driven interfaces that connect the
// application core to its adapters. Concrete implementations live in
// internal/adapter/*; only the core and transport layers use these interfaces.
package ports

import (
	"context"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// Store is the persistence port used by the replica and recovery scan.
type Store interface {
	GetKey(ctx context.Context, partitionId uint32, key string) (crdt.AWLWWMap, error)
	GetField(ctx context.Context, partitionId uint32, key, field string) (crdt.FieldEntry, bool, error)
	SaveBatch(ctx context.Context, batch []crdt.FieldUpdate) error
	// DeleteBatch physically removes the given (partition, key, field) records
	// in a single atomic transaction. Only PartitionID, Key, and Field are
	// read from each FieldUpdate; Entry is ignored. Used by tombstone GC.
	DeleteBatch(ctx context.Context, ids []crdt.FieldUpdate) error
	IteratePartition(ctx context.Context, partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error
	IterateAll(ctx context.Context, fn func(key, field string, entry crdt.FieldEntry) error) error

	// SaveCheckpoint persists a coarse HLC high-water mark to a reserved key
	// (see keyspace.CheckpointKey), allowing RecoverHLCFloor to seed the HLC
	// in O(1) on a normal restart instead of scanning every entry.
	SaveCheckpoint(ctx context.Context, ts hlc.Timestamp) error
	// LoadCheckpoint returns the most recently saved checkpoint, or
	// found=false if none has ever been written (e.g. a fresh DATA_DIR).
	LoadCheckpoint(ctx context.Context) (ts hlc.Timestamp, found bool, err error)
}
