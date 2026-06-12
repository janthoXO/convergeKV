package proto_test

import (
	"encoding/json"
	"testing"
	"testing/quick"

	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		field string
		entry crdt.FieldEntry
	}{
		{
			name:  "live entry",
			key:   "user:1",
			field: "name",
			entry: crdt.FieldEntry{
				Value:     json.RawMessage(`"Alice"`),
				Timestamp: hlc.Timestamp{PhysicalMs: 1000, Logical: 3},
				ReplicaID: "node-1",
				Deleted:   false,
			},
		},
		{
			name:  "tombstone",
			key:   "doc:99",
			field: "content",
			entry: crdt.FieldEntry{
				Value:     nil,
				Timestamp: hlc.Timestamp{PhysicalMs: 9999, Logical: 0},
				ReplicaID: "node-2",
				Deleted:   true,
			},
		},
		{
			name:  "multi-byte UTF-8 key and field",
			key:   "café:résumé",
			field: "données",
			entry: crdt.FieldEntry{
				Value:     json.RawMessage(`{"nested":true}`),
				Timestamp: hlc.Timestamp{PhysicalMs: 42, Logical: 7},
				ReplicaID: "réplique-A",
				Deleted:   false,
			},
		},
		{
			name:  "zero timestamp",
			key:   "k",
			field: "f",
			entry: crdt.FieldEntry{
				Value:     json.RawMessage(`0`),
				Timestamp: hlc.Timestamp{},
				ReplicaID: "",
				Deleted:   false,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pb := reproto.EntryToProto(tc.key, tc.field, tc.entry)
			got := reproto.ProtoToEntry(pb)

			if pb.GetKey() != tc.key {
				t.Errorf("key: got %q, want %q", pb.GetKey(), tc.key)
			}
			if pb.GetField() != tc.field {
				t.Errorf("field: got %q, want %q", pb.GetField(), tc.field)
			}
			if got.ReplicaID != tc.entry.ReplicaID {
				t.Errorf("ReplicaID: got %q, want %q", got.ReplicaID, tc.entry.ReplicaID)
			}
			if got.Timestamp.PhysicalMs != tc.entry.Timestamp.PhysicalMs {
				t.Errorf("PhysMs: got %d, want %d", got.Timestamp.PhysicalMs, tc.entry.Timestamp.PhysicalMs)
			}
			if got.Timestamp.Logical != tc.entry.Timestamp.Logical {
				t.Errorf("Logical: got %d, want %d", got.Timestamp.Logical, tc.entry.Timestamp.Logical)
			}
			if got.Deleted != tc.entry.Deleted {
				t.Errorf("Deleted: got %v, want %v", got.Deleted, tc.entry.Deleted)
			}
		})
	}
}

func TestRoundTripProperty(t *testing.T) {
	// quick.Check generates random FieldEntry values and verifies the round-trip.
	type input struct {
		PhysMs    uint64
		Logical   uint32
		ReplicaID string
		Deleted   bool
	}

	f := func(in input) bool {
		e := crdt.FieldEntry{
			Value:     json.RawMessage(`"v"`),
			Timestamp: hlc.Timestamp{PhysicalMs: in.PhysMs, Logical: in.Logical},
			ReplicaID: in.ReplicaID,
			Deleted:   in.Deleted,
		}
		pb := reproto.EntryToProto("k", "f", e)
		got := reproto.ProtoToEntry(pb)
		return got.Timestamp == e.Timestamp &&
			got.ReplicaID == e.ReplicaID &&
			got.Deleted == e.Deleted
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
