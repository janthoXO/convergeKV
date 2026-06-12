package crdt

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

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

func checkField(t *testing.T, m AWLWWMap, field, wantValue string, wantPhys uint64, wantLogical uint32, wantReplica string, wantDeleted bool) {
	t.Helper()
	e, ok := m.Fields[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	if !bytes.Equal(e.Value, json.RawMessage(wantValue)) {
		t.Errorf("field %q: value=%s, want %s", field, string(e.Value), wantValue)
	}
	if e.Timestamp.PhysicalMs != wantPhys || e.Timestamp.Logical != wantLogical {
		t.Errorf("field %q: timestamp=%+v, want {%d,%d}", field, e.Timestamp, wantPhys, wantLogical)
	}
	if e.ReplicaID != wantReplica {
		t.Errorf("field %q: replicaID=%s, want %s", field, e.ReplicaID, wantReplica)
	}
	if e.Deleted != wantDeleted {
		t.Errorf("field %q: deleted=%v, want %v", field, e.Deleted, wantDeleted)
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

// TestWinsOver covers the LWW tie-breaking rules used inline throughout node/.
func TestWinsOver(t *testing.T) {
	cases := []struct {
		name  string
		a, b  FieldEntry
		aWins bool
	}{
		{"higher physical wins", mkEntry(`1`, 10, 0, "r1", false), mkEntry(`2`, 5, 0, "r1", false), true},
		{"lower physical loses", mkEntry(`1`, 5, 0, "r1", false), mkEntry(`2`, 10, 0, "r1", false), false},
		{"same physical, higher logical wins", mkEntry(`1`, 10, 2, "r1", false), mkEntry(`2`, 10, 1, "r1", false), true},
		{"same ts, higher replicaID wins (r2>r1)", mkEntry(`1`, 10, 0, "r2", false), mkEntry(`2`, 10, 0, "r1", false), true},
		{"tombstone beats live on higher ts", mkEntry("", 10, 0, "r1", true), mkEntry(`1`, 5, 0, "r1", false), true},
		{"live beats tombstone on higher ts", mkEntry(`1`, 10, 0, "r1", false), mkEntry("", 5, 0, "r1", true), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := WinsOver(tc.a, tc.b)
			if got != tc.aWins {
				t.Errorf("WinsOver: got %v, want %v", got, tc.aWins)
			}
		})
	}
}
