package crdt

import (
	"encoding/json"
	"reflect"
	"testing"
	"github.com/janthoXO/convergeKV/internal/hlc"
)

func assertEqualMap(t *testing.T, expected, actual AWLWWMap, msg string) {
	t.Helper()
	if !reflect.DeepEqual(expected.Fields, actual.Fields) {
		t.Errorf("%s: expected %v, got %v", msg, expected.Fields, actual.Fields)
	}
}

func TestMerge(t *testing.T) {
	// Setup tests
	tests := []struct {
		name     string
		a        AWLWWMap
		b        AWLWWMap
		expected AWLWWMap
	}{
		{
			name: "Field only in A",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"name": {Value: json.RawMessage(`"Alice"`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			b: NewAWLWWMap(),
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"name": {Value: json.RawMessage(`"Alice"`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
		},
		{
			name: "Field only in B",
			a:    NewAWLWWMap(),
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"age": {Value: json.RawMessage(`30`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"age": {Value: json.RawMessage(`30`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
		},
		{
			name: "Non-overlapping fields",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"name": {Value: json.RawMessage(`"Alice"`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"age": {Value: json.RawMessage(`30`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"name": {Value: json.RawMessage(`"Alice"`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
				"age":  {Value: json.RawMessage(`30`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
		},
		{
			name: "Same field, A wins on timestamp",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`2`), Timestamp: hlc.Timestamp{PhysicalMs: 5, Logical: 1}, ReplicaID: "r2"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
		},
		{
			name: "Same field, B wins on timestamp",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 5, Logical: 1}, ReplicaID: "r1"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`2`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`2`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
		},
		{
			name: "Same field, same timestamp, A wins replica_id",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`2`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
		},
		{
			name: "Tombstone beats live entry (higher ts)",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 5, Logical: 1}, ReplicaID: "r1"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Deleted: true, Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Deleted: true, Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r2"},
			}},
		},
		{
			name: "Live entry beats tombstone (higher ts)",
			a: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
			b: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Deleted: true, Timestamp: hlc.Timestamp{PhysicalMs: 5, Logical: 1}, ReplicaID: "r2"},
			}},
			expected: AWLWWMap{Fields: map[string]FieldEntry{
				"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := Merge(tc.a, tc.b)
			assertEqualMap(t, tc.expected, res, "Merge(a,b)")

			// Commutativity checking explicitly in loop
			resCommutative := Merge(tc.b, tc.a)
			assertEqualMap(t, tc.expected, resCommutative, "Merge(b,a)")
		})
	}
}

func TestMergeProperties(t *testing.T) {
	// Idempotence
	t.Run("Idempotence", func(t *testing.T) {
		a := AWLWWMap{Fields: map[string]FieldEntry{
			"v": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
		}}
		res := Merge(a, a)
		assertEqualMap(t, a, res, "Merge(a,a) == a")
	})

	// Associativity
	t.Run("Associativity", func(t *testing.T) {
		a := AWLWWMap{Fields: map[string]FieldEntry{
			"v1": {Value: json.RawMessage(`1`), Timestamp: hlc.Timestamp{PhysicalMs: 10, Logical: 1}, ReplicaID: "r1"},
		}}
		b := AWLWWMap{Fields: map[string]FieldEntry{
			"v2": {Value: json.RawMessage(`2`), Timestamp: hlc.Timestamp{PhysicalMs: 20, Logical: 1}, ReplicaID: "r2"},
		}}
		c := AWLWWMap{Fields: map[string]FieldEntry{
			"v3": {Value: json.RawMessage(`3`), Timestamp: hlc.Timestamp{PhysicalMs: 30, Logical: 1}, ReplicaID: "r3"},
		}}

		res1 := Merge(Merge(a, b), c)
		res2 := Merge(a, Merge(b, c))

		assertEqualMap(t, res1, res2, "Merge(Merge(a, b), c) == Merge(a, Merge(b, c))")
	})
}
