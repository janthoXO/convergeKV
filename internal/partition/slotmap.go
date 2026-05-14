// Package partition owns the fixed-slot ownership map for ConvergeKV.
// It replaces the consistent-hash vnode ring from Release 2.
//
// Design:
//   - 4096 slots (NSlots). A key maps to exactly one slot via sha256 % NSlots.
//   - Each slot has an explicit list of node IDs (its replica set).
//   - The SlotMap is versioned; the higher version wins on gossip conflict.
//   - Merkle leaf index == slot index by construction (both use NSlots = 4096).
package partition

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
)

// NSlots is the fixed number of slots. Must match merkle.NumPartitions.
// Both constants use the same value so that Merkle leaf i == slot i.
const NSlots = 4096

// SlotMap is the authoritative slot ownership table.
// It is immutable once created; all mutation returns a new SlotMap.
type SlotMap struct {
	Version uint64     `json:"version"`
	Slots   [][]string `json:"slots"` // len == NSlots; Slots[i] = replica IDs for slot i
}

// SlotIndex maps a key to a slot index in [0, NSlots).
// This is the single authoritative hash function for key→slot.
// internal/merkle/bucket.go calls this directly so there is only one implementation.
func SlotIndex(key string) int {
	h := sha256.Sum256([]byte(key))
	v := binary.BigEndian.Uint64(h[:8])
	return int(v % NSlots)
}

// ReplicasForKey returns the replica ID list for the slot owning key.
func (sm SlotMap) ReplicasForKey(key string) []string {
	return sm.ReplicasForSlot(SlotIndex(key))
}

// ReplicasForSlot returns the replica ID list for slot i.
// Returns nil if i is out of range or the map has no slots.
func (sm SlotMap) ReplicasForSlot(slot int) []string {
	if slot < 0 || slot >= len(sm.Slots) {
		return nil
	}
	return sm.Slots[slot]
}

// IsReplica returns true if nodeID is in the replica list for the slot owning key.
func (sm SlotMap) IsReplica(key, nodeID string) bool {
	return sm.IsReplicaForSlot(SlotIndex(key), nodeID)
}

// IsReplicaForSlot returns true if nodeID is in the replica list for slot i.
func (sm SlotMap) IsReplicaForSlot(slot int, nodeID string) bool {
	for _, id := range sm.ReplicasForSlot(slot) {
		if id == nodeID {
			return true
		}
	}
	return false
}

// SharedSlots returns all slot indices where both nodeA and nodeB appear in the
// replica list. This replaces ring.SharedPartitions.
// Complexity: O(NSlots × RF).
func (sm SlotMap) SharedSlots(nodeA, nodeB string) []int {
	var out []int
	for i, replicas := range sm.Slots {
		hasA, hasB := false, false
		for _, id := range replicas {
			if id == nodeA {
				hasA = true
			}
			if id == nodeB {
				hasB = true
			}
		}
		if hasA && hasB {
			out = append(out, i)
		}
	}
	return out
}

// Merge resolves two SlotMaps into one according to these rules:
//
//  1. Higher version wins unconditionally.
//  2. Equal version + identical content → return self (idempotent, no version bump).
//  3. Equal version + different content → deterministic per-slot union merge with
//     version bumped by one.
//
// Rule 3 prevents split-brain when two nodes independently compute a rebalance
// from the same base version and gossip both results to the cluster. Every node
// applies the same deterministic merge and converges to the same map.
//
// Per-slot merge algorithm:
//   - Union the two replica lists for each slot.
//   - Sort the union lexicographically (deterministic, commutative).
//   - Trim to max(len(a), len(b)) entries, preserving the intended RF.
func (sm SlotMap) Merge(other SlotMap) SlotMap {
	if other.Version > sm.Version {
		return other
	}
	if other.Version < sm.Version {
		return sm
	}
	// Same version. Check for content equality.
	if slotsEqual(sm.Slots, other.Slots) {
		return sm // identical — no change
	}
	// Different content at the same version: deterministic union merge.
	return SlotMap{
		Version: sm.Version + 1,
		Slots:   mergeSlots(sm.Slots, other.Slots),
	}
}

// slotsEqual returns true iff a and b have identical content.
func slotsEqual(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	
	return true
}

// mergeSlots produces a per-slot union of two slot lists.
// For each slot the union is sorted lexicographically and trimmed to
// max(len(a[i]), len(b[i])) entries so no slot ends up under-replicated.
func mergeSlots(a, b [][]string) [][]string {
	n := max(len(b), len(a))
	out := make([][]string, n)
	for i := range n {
		var ai, bi []string
		if i < len(a) {
			ai = a[i]
		}
		if i < len(b) {
			bi = b[i]
		}
		targetLen := len(ai)
		if len(bi) > targetLen {
			targetLen = len(bi)
		}
		// Build the union (deduplicated).
		seen := make(map[string]struct{}, targetLen*2)
		for _, id := range ai {
			seen[id] = struct{}{}
		}
		for _, id := range bi {
			seen[id] = struct{}{}
		}
		union := make([]string, 0, len(seen))
		for id := range seen {
			union = append(union, id)
		}
		sort.Strings(union) // deterministic
		// Trim to the intended RF.
		if len(union) > targetLen {
			union = union[:targetLen]
		}
		out[i] = union
	}
	return out
}

// Encode serialises the SlotMap to JSON.
func (sm SlotMap) Encode() ([]byte, error) {
	return json.Marshal(sm)
}

// Decode deserialises a SlotMap from JSON.
func Decode(b []byte) (SlotMap, error) {
	var sm SlotMap
	if err := json.Unmarshal(b, &sm); err != nil {
		return SlotMap{}, fmt.Errorf("partition: decode slot map: %w", err)
	}
	if len(sm.Slots) != NSlots {
		return SlotMap{}, fmt.Errorf("partition: slot map has %d slots, want %d", len(sm.Slots), NSlots)
	}
	return sm, nil
}
