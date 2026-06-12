package replica

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// Put writes a JSON-object value to key. Each field is stored as an independent
// CRDT entry. Returns the HLC timestamp and the exact entries written, or
// ErrNotOwned if the partition is not served by this node.
//
// Writes to the same partition are serialised by p.mu; writes to different
// partitions proceed concurrently.
func (r *Replica) Put(ctx context.Context, partitionId uint32, key, valueJSON string) (hlc.Timestamp, []crdt.FieldUpdate, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: value must be a JSON object: %w", err)
	}

	p, err := r.partitionFor(partitionId)
	if err != nil {
		return hlc.Timestamp{}, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.doPut(ctx, p, key, obj)
}

func (r *Replica) doPut(ctx context.Context, p *partition, key string, obj map[string]json.RawMessage) (hlc.Timestamp, []crdt.FieldUpdate, error) {
	if p.closed {
		return hlc.Timestamp{}, nil, ErrNotOwned
	}

	ts, err := r.clock.Send()
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: hlc: %w", err)
	}

	existing, err := r.store.GetKey(ctx, p.id, key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: read: %w", err)
	}

	batch := make([]crdt.FieldUpdate, 0, len(obj))
	for field, raw := range obj {
		batch = append(batch, crdt.FieldUpdate{
			PartitionID: p.id,
			Key:         key,
			Field:       field,
			Entry: crdt.FieldEntry{
				Value:     raw,
				Timestamp: ts,
				ReplicaID: r.replicaID,
				Deleted:   false,
			},
		})
	}

	if err := r.store.SaveBatch(ctx, batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: save: %w", err)
	}

	for _, u := range batch {
		if old, had := existing.Fields[u.Field]; had {
			p.iblt.Delete(domiblt.SerialiseItem(key, u.Field, old))
		}
		p.iblt.Insert(domiblt.SerialiseItem(key, u.Field, u.Entry))
	}

	return ts, batch, nil
}

// Delete marks all current fields of key as tombstones. Returns the HLC
// timestamp and the tombstone entries written, or ErrNotOwned if the partition
// is not served by this node.
func (r *Replica) Delete(ctx context.Context, partitionId uint32, key string) (hlc.Timestamp, []crdt.FieldUpdate, error) {
	p, err := r.partitionFor(partitionId)
	if err != nil {
		return hlc.Timestamp{}, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.doDelete(ctx, p, key)
}

func (r *Replica) doDelete(ctx context.Context, p *partition, key string) (hlc.Timestamp, []crdt.FieldUpdate, error) {
	if p.closed {
		return hlc.Timestamp{}, nil, ErrNotOwned
	}

	ts, err := r.clock.Send()
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: hlc: %w", err)
	}

	m, err := r.store.GetKey(ctx, p.id, key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: read: %w", err)
	}
	if len(m.Fields) == 0 {
		return ts, nil, nil
	}

	batch := make([]crdt.FieldUpdate, 0, len(m.Fields))
	for field := range m.Fields {
		batch = append(batch, crdt.FieldUpdate{
			PartitionID: p.id,
			Key:         key,
			Field:       field,
			Entry: crdt.FieldEntry{
				Value:     nil,
				Timestamp: ts,
				ReplicaID: r.replicaID,
				Deleted:   true,
			},
		})
	}

	if err := r.store.SaveBatch(ctx, batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: save: %w", err)
	}

	for _, u := range batch {
		p.iblt.Delete(domiblt.SerialiseItem(key, u.Field, m.Fields[u.Field]))
		p.iblt.Insert(domiblt.SerialiseItem(key, u.Field, u.Entry))
	}

	return ts, batch, nil
}

// ApplyDelta merges a single incoming field entry from a peer. Advances the
// HLC with the remote timestamp regardless of whether the partition is owned —
// a wasted advance is harmless; clock-going-backwards is not. Returns
// ErrNotOwned if the partition is not served by this node, true if the
// incoming entry actually changed local state otherwise.
func (r *Replica) ApplyDelta(ctx context.Context, partitionId uint32, key, field string, incoming crdt.FieldEntry) (bool, error) {
	if _, err := r.clock.Receive(incoming.Timestamp); err != nil {
		return false, err
	}
	if r.isExpiredTombstone(incoming) {
		// Drop ancient incoming tombstones rather than re-persisting them: a
		// node that already GC'd this tombstone shouldn't re-import it from a
		// peer that hasn't GC'd yet, and a full-partition fallback shouldn't
		// re-seed a freshly cleaned partition with stale tombstones.
		return false, nil
	}
	p, err := r.partitionFor(partitionId)
	if err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.doApply(ctx, p, key, field, incoming)
}

func (r *Replica) doApply(ctx context.Context, p *partition, key, field string, incoming crdt.FieldEntry) (bool, error) {
	if p.closed {
		return false, ErrNotOwned
	}

	existing, exists, err := r.store.GetField(ctx, p.id, key, field)
	if err != nil {
		return false, err
	}
	if exists && !crdt.WinsOver(incoming, existing) {
		return false, nil
	}

	if err := r.store.SaveBatch(ctx, []crdt.FieldUpdate{
		{PartitionID: p.id, Key: key, Field: field, Entry: incoming},
	}); err != nil {
		return false, err
	}

	if exists {
		p.iblt.Delete(domiblt.SerialiseItem(key, field, existing))
	}
	p.iblt.Insert(domiblt.SerialiseItem(key, field, incoming))
	return true, nil
}
