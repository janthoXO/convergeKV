// Package grpcsrv implements the SyncService gRPC server (inbound replication).
package grpcsrv

import (
	"context"
	"io"
	"log/slog"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Node is the subset of the local replica that the SyncService server needs.
//
// StateOverview returns opaque bytes describing the partition's contents for
// the anti-entropy initiator to diff against. The wire format is currently an
// encoded IBLT but the handler does not depend on that.
type Node interface {
	Owns(partitionId uint32) bool
	StateOverview(partitionId uint32) []byte
	ApplyDelta(ctx context.Context, partitionId uint32, key, field string, e crdt.FieldEntry) (bool, error)
	IteratePartition(ctx context.Context, partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error
	GetField(ctx context.Context, partitionId uint32, key, field string) (crdt.FieldEntry, bool, error)
}

// Handler implements repb.SyncServiceServer.
type Handler struct {
	repb.UnimplementedSyncServiceServer
	node Node
}

// New returns a Handler ready to register with a gRPC server.
func New(n Node) *Handler {
	return &Handler{node: n}
}

// GetStateOverview returns the node's opaque state overview for the requested
// partition. The bytes are mechanism-defined (currently an encoded IBLT).
func (h *Handler) GetStateOverview(_ context.Context, req *repb.StateOverviewRequest) (*repb.StateOverviewResponse, error) {
	partitionId := req.GetPartitionId()
	if err := h.validatePID(partitionId); err != nil {
		return nil, err
	}

	overview := h.node.StateOverview(partitionId)
	slog.Debug("GetStateOverview", "partitionId", partitionId, "peer", req.GetReplicaId(), "bytes", len(overview))
	return &repb.StateOverviewResponse{OverviewData: overview}, nil
}

// PushEntries receives a client-streaming batch of delta entries.
// The first chunk must be a PushHeader declaring the partition.
func (h *Handler) PushEntries(stream grpc.ClientStreamingServer[repb.PushChunk, repb.PushAck]) error {
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
	if err := h.validatePID(partitionId); err != nil {
		return err
	}

	ctx := stream.Context()
	applied := int32(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch p := chunk.GetPayload().(type) {
		case *repb.PushChunk_Header:
			// Long-lived streams send a new header before each batch to reset partitionId.
			partitionId = p.Header.GetPartitionId()
			if err := h.validatePID(partitionId); err != nil {
				return err
			}

		case *repb.PushChunk_Entry:
			d := p.Entry
			if err := validateEntryFields(d.GetKey(), d.GetField()); err != nil {
				slog.Warn("PushEntries rejected", "key", d.GetKey(), "field", d.GetField(), "err", err)
				continue
			}

			changed, err := h.node.ApplyDelta(ctx, partitionId, d.GetKey(), d.GetField(), reproto.ProtoToEntry(d))
			if err != nil {
				slog.Warn("PushEntries apply failed", "key", d.GetKey(), "field", d.GetField(), "err", err)
				continue
			}

			if changed {
				applied++
			}
		}
	}

	return stream.SendAndClose(&repb.PushAck{Applied: applied})
}

// PullEntries streams entries back to the initiator.
// Empty Identifiers means "send the full partition state" (fallback path).
func (h *Handler) PullEntries(req *repb.PullRequest, stream grpc.ServerStreamingServer[repb.DeltaEntry]) error {
	partitionId := req.GetPartitionId()
	err := h.validatePID(partitionId)
	if err != nil {
		return err
	}
	ctx := stream.Context()

	if len(req.GetIdentifiers()) == 0 {
		// send full partition state
		sent := 0
		err = h.node.IteratePartition(ctx, partitionId, func(key, field string, entry crdt.FieldEntry) error {
			sent++
			return stream.Send(reproto.EntryToProto(key, field, entry))
		})
		if err != nil {
			return err
		}

		slog.Debug("PullEntries full", "partitionId", partitionId, "peer", req.GetReplicaId(), "sent", sent)
		return nil
	}

	sent := 0
	for _, id := range req.GetIdentifiers() {
		entry, found, err := h.node.GetField(ctx, partitionId, id.GetKey(), id.GetField())
		if err != nil {
			slog.Warn("PullEntries GetField failed", "key", id.GetKey(), "err", err)
			continue
		}
		if !found {
			continue
		}

		err = stream.Send(reproto.EntryToProto(id.GetKey(), id.GetField(), entry))
		if err != nil {
			return err
		}

		sent++
	}
	slog.Debug("PullEntries partial", "partitionId", partitionId, "peer", req.GetReplicaId(), "sent", sent, "requested", len(req.GetIdentifiers()))
	return nil
}

// validatePID asks the node whether it currently serves this partition.
// Returns FailedPrecondition if not — distinct from InvalidArgument so the
// initiator can tell "partition migrated" from "you sent garbage".
func (h *Handler) validatePID(partitionId uint32) error {
	if !h.node.Owns(partitionId) {
		return status.Errorf(codes.FailedPrecondition, "partition %d not owned by this node", partitionId)
	}
	return nil
}

func validateEntryFields(key, field string) error {
	if err := keyspace.RejectNullBytes(key); err != nil {
		return status.Error(codes.InvalidArgument, "key contains null byte")
	}
	if err := keyspace.RejectNullBytes(field); err != nil {
		return status.Error(codes.InvalidArgument, "field contains null byte")
	}
	return nil
}
