package iblt_test

import (
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// TestSerialiseItemRoundTrip verifies the canonical IBLT-item encoding is
// invertible: Serialise → Deserialise yields the same components.
func TestSerialiseItemRoundTrip(t *testing.T) {
	ts := hlc.Timestamp{PhysicalMs: 1234567890, Logical: 42}
	entry := crdt.FieldEntry{
		Value:     json.RawMessage(`"hello"`),
		Timestamp: ts,
		ReplicaID: "replica-abc",
		Deleted:   false,
	}

	bytes := domiblt.SerialiseItem("my-key", "my-field", entry)
	k, f, rID, physMs, logical, deleted, ok := domiblt.DeserialiseItem(bytes)
	if !ok {
		t.Fatal("DeserialiseItem returned invalid")
	}
	if k != "my-key" || f != "my-field" || rID != "replica-abc" ||
		physMs != ts.PhysicalMs || logical != ts.Logical || deleted {
		t.Errorf("deserialised mismatch: k=%s f=%s rID=%s physMs=%d logical=%d deleted=%v",
			k, f, rID, physMs, logical, deleted)
	}
}

// TestSerialiseItemTombstoneRoundTrip exercises the Deleted=true path so the
// trailing tombstone-flag byte is preserved through the encoding.
func TestSerialiseItemTombstoneRoundTrip(t *testing.T) {
	entry := crdt.FieldEntry{
		Timestamp: hlc.Timestamp{PhysicalMs: 1, Logical: 0},
		ReplicaID: "r",
		Deleted:   true,
	}
	bytes := domiblt.SerialiseItem("k", "f", entry)
	_, _, _, _, _, deleted, ok := domiblt.DeserialiseItem(bytes)
	if !ok || !deleted {
		t.Errorf("tombstone flag lost: ok=%v deleted=%v", ok, deleted)
	}
}
