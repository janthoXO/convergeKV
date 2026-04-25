package replication

import (
	"context"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/node"
)

// Handler implements repb.ReplicationServiceServer.
type Handler struct {
	repb.UnimplementedReplicationServiceServer
	node    *node.Node
	context *CausalContext
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node, ctx *CausalContext) *Handler {
	return &Handler{node: n, context: ctx}
}

// Sync is called by a peer during anti-entropy.
// It returns all delta entries that the caller's causal context indicates it hasn't seen.
func (h *Handler) Sync(_ context.Context, req *repb.SyncRequest) (*repb.SyncResponse, error) {
	// Build the peer's seen map from the request.
	peerSeen := make(map[string]hlc.Timestamp)
	for rid, pts := range req.Context.GetSeen() {
		peerSeen[rid] = hlc.Timestamp{
			PhysicalMs: pts.GetPhysicalMs(),
			Logical:    pts.GetLogical(),
		}
	}

	// Collect all local entries the peer hasn't seen yet.
	snapshot := h.node.Snapshot()
	var deltas []*repb.DeltaEntry
	for _, rec := range snapshot {
		peerHWM, ok := peerSeen[rec.Entry.ReplicaID]
		if !ok || hlc.Less(peerHWM, rec.Entry.Timestamp) {
			deltas = append(deltas, encodeEntry(rec))
		}
	}

	// Return our own causal context so the peer can update its view.
	localCtx := h.context.Snapshot()
	seenPb := make(map[string]*kvpb.HLCTimestamp, len(localCtx))
	for rid, ts := range localCtx {
		seenPb[rid] = &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical}
	}

	return &repb.SyncResponse{
		Deltas:  deltas,
		Context: &repb.CausalContext{Seen: seenPb},
	}, nil
}

// encodeEntry converts a DeltaRecord into a protobuf DeltaEntry for the wire.
func encodeEntry(rec node.DeltaRecord) *repb.DeltaEntry {
	return &repb.DeltaEntry{
		Key:       rec.Key,
		Field:     rec.Field,
		ValueJson: rec.Entry.Value,
		Timestamp: &kvpb.HLCTimestamp{
			PhysicalMs: rec.Entry.Timestamp.PhysicalMs,
			Logical:    rec.Entry.Timestamp.Logical,
		},
		ReplicaId: rec.Entry.ReplicaID,
		Deleted:   rec.Entry.Deleted,
	}
}
