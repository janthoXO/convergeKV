package grpc

import (
	"context"
	"crypto/subtle"

	debugpb "github.com/janthoXO/convergeKV/gen/debug"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DebugStore is the subset of ports.Store needed by the debug handler.
type DebugStore interface {
	IterateAll(ctx context.Context, fn func(key, field string, entry crdt.FieldEntry) error) error
}

// DebugHandler implements debugpb.DebugServiceServer.
// It is registered only when DEBUG_ENABLED=true and requires a non-empty
// static bearer token on every call.
type DebugHandler struct {
	debugpb.UnimplementedDebugServiceServer
	store DebugStore
	token string // required bearer token; must be non-empty (enforced at construction)
}

// NewDebugHandler returns a ready-to-register DebugHandler.
func NewDebugHandler(store DebugStore, token string) *DebugHandler {
	return &DebugHandler{store: store, token: token}
}

// ScanAll streams every persisted (key, field) CRDT entry on this node.
func (h *DebugHandler) ScanAll(req *debugpb.ScanRequest, stream grpc.ServerStreamingServer[debugpb.DebugEntry]) error {
	if err := h.auth(stream.Context()); err != nil {
		return err
	}
	return h.store.IterateAll(stream.Context(), func(key, field string, entry crdt.FieldEntry) error {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		return stream.Send(&debugpb.DebugEntry{
			Key:       key,
			Field:     field,
			ValueJson: string(entry.Value),
			Timestamp: &kvpb.HLCTimestamp{
				PhysicalMs: entry.Timestamp.PhysicalMs,
				Logical:    entry.Timestamp.Logical,
			},
			ReplicaId: entry.ReplicaID,
			Deleted:   entry.Deleted,
		})
	})
}

func (h *DebugHandler) auth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	want := "Bearer " + h.token
	if len(vals) == 0 || subtle.ConstantTimeCompare([]byte(vals[0]), []byte(want)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid debug token")
	}
	return nil
}
