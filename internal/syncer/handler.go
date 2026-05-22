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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// GetIBLT returns a snapshot of the local IBLT for the requested partition so
// the initiator can compute the symmetric difference on its own side.
func (h *Handler) GetIBLT(_ context.Context, req *repb.GetIBLTRequest) (*repb.GetIBLTResponse, error) {
	partitionId := req.GetPartitionId()
	encoded := h.node.IBLTSnapshot(partitionId).Encode()
	log.Printf("[syncer/handler] GetIBLT partitionId=%d from %s: sent %d bytes", partitionId, req.GetReplicaId(), len(encoded))
	return &repb.GetIBLTResponse{IbltData: encoded}, nil
}

// PushEntries receives a client-side stream of PushChunk messages. The first
// chunk must be a PushHeader declaring the partition; subsequent chunks are
// DeltaEntry records to merge. The stream is rejected with InvalidArgument if
// it does not open with a header.
func (h *Handler) PushEntries(stream grpc.ClientStreamingServer[repb.PushChunk, repb.PushAck]) error {
	// ── First chunk: must be a partition header ──────────────────────────────
	first, err := stream.Recv()
	if err == io.EOF {
		return stream.SendAndClose(&repb.PushAck{Applied: 0})
	}
	if err != nil {
		return err
	}
	hdr, ok := first.GetPayload().(*repb.PushChunk_Header)
	if !ok || hdr == nil {
		return status.Error(codes.InvalidArgument, "first PushChunk must be a PushHeader")
	}
	partitionId := hdr.Header.GetPartitionId()
	log.Printf("[syncer/handler] PushEntries partitionId=%d stream opened", partitionId)

	// ── Subsequent chunks: DeltaEntry records ────────────────────────────────
	applied := int32(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entryChunk, ok := chunk.GetPayload().(*repb.PushChunk_Entry)
		if !ok || entryChunk == nil {
			log.Printf("[syncer/handler] PushEntries partitionId=%d: unexpected non-entry chunk, skipping", partitionId)
			continue
		}
		d := entryChunk.Entry
		changed, err := h.node.ApplyDelta(partitionId, d.GetKey(), d.GetField(), protoToEntry(d))
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

// PullEntries streams entries back to the initiator for the requested partition.
// If req.Identifiers is empty, the full partition state is streamed (fallback).
// Otherwise, each requested (key, field) is looked up and sent as-is.
func (h *Handler) PullEntries(req *repb.PullRequest, stream grpc.ServerStreamingServer[repb.DeltaEntry]) error {
	partitionId := req.GetPartitionId()
	if len(req.GetIdentifiers()) == 0 {
		sent := 0
		if err := h.node.IteratePartition(partitionId, func(key, field string, entry crdt.FieldEntry) error {
			sent++
			return stream.Send(entryToProto(key, field, entry))
		}); err != nil {
			log.Printf("[syncer/handler] PullEntries full-partition partitionId=%d iterate: %v", partitionId, err)
			return err
		}
		log.Printf("[syncer/handler] PullEntries full-partition partitionId=%d to %s: sent %d entries", partitionId, req.GetReplicaId(), sent)
		return nil
	}

	sent := 0
	for _, id := range req.GetIdentifiers() {
		entry, found, err := h.node.GetField(partitionId, id.GetKey(), id.GetField())
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
	log.Printf("[syncer/handler] PullEntries partitionId=%d to %s: sent %d/%d entries", partitionId, req.GetReplicaId(), sent, len(req.GetIdentifiers()))
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
