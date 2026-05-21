package syncer

import (
	"context"
	"io"
	"log"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/node"
	"google.golang.org/grpc"
)

// Handler implements repb.SyncServiceServer.
type Handler struct {
	repb.UnimplementedSyncServiceServer
	node *node.Node
}

// NewHandler returns a Handler ready to register with a gRPC server.
func NewHandler(n *node.Node) *Handler {
	return &Handler{node: n}
}

// GetIBLT returns a snapshot of the local IBLT so the initiator can compute
// the symmetric difference on its own side.
func (h *Handler) GetIBLT(_ context.Context, req *repb.GetIBLTRequest) (*repb.GetIBLTResponse, error) {
	encoded := h.node.IBLTSnapshot().Encode()
	log.Printf("[syncer/handler] GetIBLT from %s: sent %d bytes", req.GetReplicaId(), len(encoded))
	return &repb.GetIBLTResponse{IbltData: encoded}, nil
}

// PushEntries receives a client-side stream of DeltaEntry messages and applies
// each one via CRDT merge. Used for both write-path push and anti-entropy push.
func (h *Handler) PushEntries(stream grpc.ClientStreamingServer[repb.DeltaEntry, repb.PushAck]) error {
	applied := int32(0)
	for {
		d, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		changed, err := h.node.ApplyDelta(d.GetKey(), d.GetField(), protoToEntry(d))
		if err != nil {
			log.Printf("[syncer/handler] PushEntries apply key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
			continue
		}
		if changed {
			applied++
		}
	}
	return stream.SendAndClose(&repb.PushAck{Applied: applied})
}

// PullEntries streams entries back to the initiator.
// If req.Identifiers is empty, the full local state is streamed (fallback path).
// Otherwise, each requested (key, field) is looked up and sent as-is.
func (h *Handler) PullEntries(req *repb.PullRequest, stream grpc.ServerStreamingServer[repb.DeltaEntry]) error {
	if len(req.GetIdentifiers()) == 0 {
		sent := 0
		if err := h.node.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
			sent++
			return stream.Send(entryToProto(key, field, entry))
		}); err != nil {
			log.Printf("[syncer/handler] PullEntries full-state iterate: %v", err)
			return err
		}
		log.Printf("[syncer/handler] PullEntries full-state to %s: sent %d entries", req.GetReplicaId(), sent)
		return nil
	}

	sent := 0
	for _, id := range req.GetIdentifiers() {
		entry, found, err := h.node.GetField(id.GetKey(), id.GetField())
		if err != nil {
			log.Printf("[syncer/handler] PullEntries GetField key=%s field=%s: %v", id.GetKey(), id.GetField(), err)
			continue
		}
		if !found {
			continue
		}
		if err := stream.Send(entryToProto(id.GetKey(), id.GetField(), entry)); err != nil {
			return err
		}
		sent++
	}
	log.Printf("[syncer/handler] PullEntries to %s: sent %d/%d entries", req.GetReplicaId(), sent, len(req.GetIdentifiers()))
	return nil
}

// entryToProto encodes a (key, field, entry) triple into a protobuf DeltaEntry.
func entryToProto(key, field string, e crdt.FieldEntry) *repb.DeltaEntry {
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

// protoToEntry decodes a DeltaEntry proto into a crdt.FieldEntry.
func protoToEntry(d *repb.DeltaEntry) crdt.FieldEntry {
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
