// Package proto converts between domain types and protobuf messages.
package proto

import (
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// EntryToProto encodes a (key, field, entry) triple into a protobuf DeltaEntry.
func EntryToProto(key, field string, e crdt.FieldEntry) *repb.DeltaEntry {
	return &repb.DeltaEntry{
		Key:       key,
		Field:     field,
		ValueJson: e.Value,
		Timestamp: &kvpb.HLCTimestamp{
			PhysicalMs: e.Timestamp.PhysicalMs,
			Logical:    e.Timestamp.Logical,
		},
		ReplicaId: e.ReplicaID,
		Deleted:   e.Deleted,
	}
}

// ProtoToEntry decodes a DeltaEntry proto into a crdt.FieldEntry.
func ProtoToEntry(d *repb.DeltaEntry) crdt.FieldEntry {
	return crdt.FieldEntry{
		Value: d.GetValueJson(),
		Timestamp: hlc.Timestamp{
			PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
			Logical:    d.GetTimestamp().GetLogical(),
		},
		ReplicaID: d.GetReplicaId(),
		Deleted:   d.GetDeleted(),
	}
}

// HLCToProto converts an HLC Timestamp to its protobuf representation.
func HLCToProto(ts hlc.Timestamp) *kvpb.HLCTimestamp {
	return &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical}
}

// ProtoToHLC converts a protobuf HLCTimestamp to its domain representation.
// A nil input yields the zero Timestamp.
func ProtoToHLC(ts *kvpb.HLCTimestamp) hlc.Timestamp {
	return hlc.Timestamp{PhysicalMs: ts.GetPhysicalMs(), Logical: ts.GetLogical()}
}
