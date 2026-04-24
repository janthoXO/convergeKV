package crdt

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/hlc"
)

// mapsEqual compares two AWLWWMaps field-by-field for equality.
func mapsEqual(a, b AWLWWMap) bool {
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for field, ae := range a.Fields {
		be, ok := b.Fields[field]
		if !ok {
			return false
		}
		if !bytes.Equal(ae.Value, be.Value) {
			return false
		}
		if !hlc.Equal(ae.Timestamp, be.Timestamp) {
			return false
		}
		if ae.ReplicaID != be.ReplicaID {
			return false
		}
		if ae.Deleted != be.Deleted {
			return false
		}
	}
	return true
}

func mkEntry(valueJSON string, physMs uint64, logical uint32, replicaID string, deleted bool) FieldEntry {
	var raw json.RawMessage
	if valueJSON != "" {
		raw = json.RawMessage(valueJSON)
	}
	return FieldEntry{
		Value:     raw,
		Timestamp: hlc.Timestamp{PhysicalMs: physMs, Logical: logical},
		ReplicaID: replicaID,
		Deleted:   deleted,
	}
}

func mapWith(fields map[string]FieldEntry) AWLWWMap {
	m := NewAWLWWMap()
	for k, v := range fields {
		m.Fields[k] = v
	}
	return m
}

// TestMergeTableDriven covers all acceptance-criteria rows.
func TestMergeTableDriven(t *testing.T) {
	tests := []struct {
		name   string
		a      AWLWWMap
		b      AWLWWMap
		check  func(t *testing.T, result AWLWWMap)
	}{
		{
			name: "Field only in A",
			a:    mapWith(map[string]FieldEntry{"name": mkEntry(`"Alice"`, 10, 0, "r1", false)}),
			b:    NewAWLWWMap(),
			check: func(t *testing.T, result AWLWWMap) {
				e, ok := result.Fields["name"]
				if !ok || string(e.Value) != `"Alice"` {
					t.Errorf("expected name=Alice, got %+v", result.Fields)
				}
			},
		},
		{
			name: "Field only in B",
			a:    NewAWLWWMap(),
			b:    mapWith(map[string]FieldEntry{"age": mkEntry(`30`, 10, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				e, ok := result.Fields["age"]
				if !ok || string(e.Value) != `30` {
					t.Errorf("expected age=30, got %+v", result.Fields)
				}
			},
		},
		{
			name: "Non-overlapping fields",
			a:    mapWith(map[string]FieldEntry{"name": mkEntry(`"Alice"`, 10, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"age": mkEntry(`30`, 10, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				if _, ok := result.Fields["name"]; !ok {
					t.Error("expected name field")
				}
				if _, ok := result.Fields["age"]; !ok {
					t.Error("expected age field")
				}
			},
		},
		{
			name: "Same field, A wins on timestamp",
			a:    mapWith(map[string]FieldEntry{"v": mkEntry(`1`, 10, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"v": mkEntry(`2`, 5, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				e := result.Fields["v"]
				if string(e.Value) != `1` {
					t.Errorf("expected v=1 (A wins), got %s", string(e.Value))
				}
			},
		},
		{
			name: "Same field, B wins on timestamp",
			a:    mapWith(map[string]FieldEntry{"v": mkEntry(`1`, 5, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"v": mkEntry(`2`, 10, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				e := result.Fields["v"]
				if string(e.Value) != `2` {
					t.Errorf("expected v=2 (B wins), got %s", string(e.Value))
				}
			},
		},
		{
			name: "Same field, same timestamp, A wins on replica_id (r2 > r1)",
			a:    mapWith(map[string]FieldEntry{"v": mkEntry(`1`, 10, 0, "r2", false)}),
			b:    mapWith(map[string]FieldEntry{"v": mkEntry(`2`, 10, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				e := result.Fields["v"]
				if string(e.Value) != `1` {
					t.Errorf("expected v=1 (r2 > r1), got %s", string(e.Value))
				}
			},
		},
		{
			name: "Tombstone beats live entry (higher ts)",
			a:    mapWith(map[string]FieldEntry{"v": mkEntry(`1`, 5, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"v": mkEntry("", 10, 0, "r1", true)}),
			check: func(t *testing.T, result AWLWWMap) {
				e := result.Fields["v"]
				if !e.Deleted {
					t.Errorf("expected tombstone to win, got %+v", e)
				}
			},
		},
		{
			name: "Live entry beats tombstone (higher ts)",
			a:    mapWith(map[string]FieldEntry{"v": mkEntry(`1`, 10, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"v": mkEntry("", 5, 0, "r1", true)}),
			check: func(t *testing.T, result AWLWWMap) {
				e := result.Fields["v"]
				if e.Deleted {
					t.Errorf("expected live entry to win, got tombstone")
				}
				if string(e.Value) != `1` {
					t.Errorf("expected v=1, got %s", string(e.Value))
				}
			},
		},
		{
			name: "Commutativity",
			a:    mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false), "y": mkEntry(`2`, 10, 0, "r2", false)}),
			b:    mapWith(map[string]FieldEntry{"x": mkEntry(`3`, 8, 0, "r1", false), "z": mkEntry(`4`, 3, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				// Rebuild with swapped args.
				a := mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false), "y": mkEntry(`2`, 10, 0, "r2", false)})
				b := mapWith(map[string]FieldEntry{"x": mkEntry(`3`, 8, 0, "r1", false), "z": mkEntry(`4`, 3, 0, "r1", false)})
				ab := Merge(a, b)
				ba := Merge(b, a)
				if !mapsEqual(ab, ba) {
					t.Errorf("Merge(a,b) != Merge(b,a): %+v vs %+v", ab, ba)
				}
			},
		},
		{
			name: "Idempotence",
			a:    mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				a := mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false)})
				aa := Merge(a, a)
				if !mapsEqual(a, aa) {
					t.Errorf("Merge(a,a) != a: %+v vs %+v", aa, a)
				}
			},
		},
		{
			name: "Associativity",
			a:    mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false)}),
			b:    mapWith(map[string]FieldEntry{"x": mkEntry(`2`, 8, 0, "r2", false)}),
			check: func(t *testing.T, result AWLWWMap) {
				a := mapWith(map[string]FieldEntry{"x": mkEntry(`1`, 5, 0, "r1", false)})
				b := mapWith(map[string]FieldEntry{"x": mkEntry(`2`, 8, 0, "r2", false)})
				c := mapWith(map[string]FieldEntry{"y": mkEntry(`3`, 3, 0, "r3", false)})
				left := Merge(Merge(a, b), c)
				right := Merge(a, Merge(b, c))
				if !mapsEqual(left, right) {
					t.Errorf("Merge(Merge(a,b),c) != Merge(a,Merge(b,c)): %+v vs %+v", left, right)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := Merge(tc.a, tc.b)
			tc.check(t, result)
		})
	}
}

// TestToJSON verifies deleted fields are omitted and non-deleted fields appear.
func TestToJSON(t *testing.T) {
	m := mapWith(map[string]FieldEntry{
		"name": mkEntry(`"Alice"`, 10, 0, "r1", false),
		"age":  mkEntry("", 5, 0, "r1", true), // tombstone
	})
	b, ok := ToJSON(m)
	if !ok {
		t.Fatal("expected ok=true")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, has := obj["age"]; has {
		t.Error("tombstone field 'age' should be omitted from JSON")
	}
	if string(obj["name"]) != `"Alice"` {
		t.Errorf("expected name=Alice, got %s", string(obj["name"]))
	}
}

// TestToJSONAllTombstones verifies all-tombstone maps return false.
func TestToJSONAllTombstones(t *testing.T) {
	m := mapWith(map[string]FieldEntry{
		"x": mkEntry("", 5, 0, "r1", true),
	})
	_, ok := ToJSON(m)
	if ok {
		t.Error("expected ok=false for all-tombstone map")
	}
}
