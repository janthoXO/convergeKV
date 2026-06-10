package iblt

import "github.com/janthoXO/convergeKV/internal/domain/crdt"

// ItemID identifies a single (key, field) CRDT entry by its IBLT item
// fields, without carrying the entry's value. It is the unit exchanged
// between Reconcile and the transport layer that turns it into a push (full
// entry) or pull (identifier-only) request.
type ItemID struct {
	Key        string
	Field      string
	ReplicaID  string
	PhysicalMs uint64
	Logical    uint32
}

// GetField looks up the current locally-stored entry for (key, field).
// found is false if the entry does not exist (e.g. it has since been
// compacted or was never present).
type GetField func(key, field string) (entry crdt.FieldEntry, found bool)

// Reconcile computes, from a local and remote IBLT snapshot for the same
// partition, the set of items the local node should push to the remote peer
// and the set it should pull from it.
//
// It is a pure function: all I/O is via getField, a synchronous lookup
// against the local store. It performs:
//  1. local.Subtract(remote) — propagates A2: an error here (size mismatch,
//     e.g. peer has a different IBLT_CELLS) is returned unchanged so the
//     caller can skip the pair rather than risk a panic or a misleading
//     full-state fallback.
//  2. diff.Decode() — if it fails (the symmetric difference is too large to
//     decode), fallback=true is returned and toPush/toPull are nil; the
//     caller should fall back to a full-partition exchange.
//  3. For each only-local item, a staleness re-check against getField: items
//     the local node has since overwritten (different timestamp/replica/
//     deleted flag) are dropped rather than pushed under a stale identity.
//  4. Only-remote items are returned as pull identifiers; the caller fetches
//     the actual entries from the peer.
func Reconcile(local, remote *IBLT, getField GetField) (toPush, toPull []ItemID, fallback bool, err error) {
	diff, err := local.Subtract(remote)
	if err != nil {
		return nil, nil, false, err
	}

	onlyLocal, onlyRemote, ok := diff.Decode()
	if !ok {
		return nil, nil, true, nil
	}

	for _, itemBytes := range onlyLocal {
		key, field, rID, physMs, logical, deleted, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found := getField(key, field)
		if !found {
			continue
		}
		if entry.Timestamp.PhysicalMs != physMs || entry.Timestamp.Logical != logical ||
			entry.ReplicaID != rID || entry.Deleted != deleted {
			continue
		}
		toPush = append(toPush, ItemID{Key: key, Field: field, ReplicaID: rID, PhysicalMs: physMs, Logical: logical})
	}

	for _, itemBytes := range onlyRemote {
		key, field, rID, physMs, logical, _, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		toPull = append(toPull, ItemID{Key: key, Field: field, ReplicaID: rID, PhysicalMs: physMs, Logical: logical})
	}

	return toPush, toPull, false, nil
}
