package crdt_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// mergeInto applies each entry from src into dst using the same LWW rule as
// the replica: candidate wins if WinsOver(candidate, existing) is true.
func mergeInto(dst *crdt.AWLWWMap, src crdt.AWLWWMap) {
	for field, entry := range src.Fields {
		existing, ok := dst.Fields[field]
		if !ok || crdt.WinsOver(entry, existing) {
			dst.Fields[field] = entry
		}
	}
}

// mapsEqual compares two AWLWWMap instances for equality (field-by-field).
func mapsEqual(a, b crdt.AWLWWMap) bool {
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for f, ea := range a.Fields {
		eb, ok := b.Fields[f]
		if !ok {
			return false
		}
		if ea.Timestamp != eb.Timestamp || ea.ReplicaID != eb.ReplicaID || ea.Deleted != eb.Deleted {
			return false
		}
		if !bytes.Equal(ea.Value, eb.Value) {
			return false
		}
	}
	return true
}

type op struct {
	replicaIdx int
	key        string
	field      string
	physMs     uint64
	logical    uint32
	replicaID  string
	value      json.RawMessage
	deleted    bool
}

// applyOp writes op into the map at maps[op.replicaIdx].
func applyOp(maps []crdt.AWLWWMap, o op) {
	entry := crdt.FieldEntry{
		Value:     o.value,
		Timestamp: hlc.Timestamp{PhysicalMs: o.physMs, Logical: o.logical},
		ReplicaID: o.replicaID,
		Deleted:   o.deleted,
	}
	m := maps[o.replicaIdx]
	existing, ok := m.Fields[o.key+":"+o.field]
	if !ok || crdt.WinsOver(entry, existing) {
		m.Fields[o.key+":"+o.field] = entry
	}
	maps[o.replicaIdx] = m
}

// gossipAll simulates full gossip: every map receives every entry from all others.
func gossipAll(maps []crdt.AWLWWMap) {
	for i := range maps {
		for j, src := range maps {
			if i == j {
				continue
			}
			mergeInto(&maps[i], src)
		}
	}
}

func TestCRDTConvergence(t *testing.T) {
	keys := []string{"user:1", "doc:99", "item:7"}
	fields := []string{"name", "age", "value"}
	replicas := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	const numOps = 200
	const numReplicas = 5

	seeds := []int64{42, 100, 999, 12345, 0xdeadbeef}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			maps := make([]crdt.AWLWWMap, numReplicas)
			for i := range maps {
				maps[i] = crdt.NewAWLWWMap()
			}

			// Generate and apply a random schedule of operations.
			for i := 0; i < numOps; i++ {
				rIdx := rng.Intn(numReplicas)
				key := keys[rng.Intn(len(keys))]
				field := fields[rng.Intn(len(fields))]
				physMs := uint64(rng.Intn(1000) + 1)
				logical := uint32(rng.Intn(10))
				deleted := rng.Intn(10) == 0 // 10% tombstone rate

				var value json.RawMessage
				if !deleted {
					value = json.RawMessage(fmt.Sprintf(`%d`, rng.Intn(100)))
				}

				applyOp(maps, op{
					replicaIdx: rIdx,
					key:        key,
					field:      field,
					physMs:     physMs,
					logical:    logical,
					replicaID:  replicas[rIdx],
					value:      value,
					deleted:    deleted,
				})
			}

			// Simulate full gossip: merge all into all (two rounds handles any
			// transitive inconsistency from single-entry merge semantics).
			gossipAll(maps)
			gossipAll(maps)

			// All replicas must converge to the same state.
			for i := 1; i < numReplicas; i++ {
				if !mapsEqual(maps[0], maps[i]) {
					t.Errorf("replica 0 and replica %d diverged after gossip", i)
				}
			}
		})
	}
}
