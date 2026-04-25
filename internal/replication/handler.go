package replication

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/node"
)

// Handler implements repb.ReplicationServiceServer.
type Handler struct {
	repb.UnimplementedReplicationServiceServer
	node *node.Node
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node) *Handler {
	return &Handler{node: n}
}

// HashSync handles Phase 1. The caller sends its bucket hashes; we reply with
// which buckets differ (so the caller knows what to request from us) and our
// own bucket hashes (so the caller can tell us what WE are missing from them).
func (h *Handler) HashSync(_ context.Context, req *repb.HashSyncRequest) (*repb.HashSyncResponse, error) {
	if len(req.BucketHashes) != merkle.NumBuckets {
		return nil, status.Errorf(codes.InvalidArgument,
			"expected %d bucket hashes, got %d", merkle.NumBuckets, len(req.BucketHashes))
	}

	// Convert wire format ([][]byte) to []merkle.Hash.
	peerHashes := make([]merkle.Hash, merkle.NumBuckets)
	for i, b := range req.BucketHashes {
		if len(b) != 32 {
			return nil, status.Errorf(codes.InvalidArgument, "bucket hash %d: want 32 bytes, got %d", i, len(b))
		}
		copy(peerHashes[i][:], b)
	}

	// Find which of our buckets differ from the peer's.
	divergent := h.node.MerkleTree().DivergentBuckets(peerHashes)
	divergentInt32 := make([]int32, len(divergent))
	for i, d := range divergent {
		divergentInt32[i] = int32(d)
	}

	// Send back our own bucket hashes so the peer can find what IT is missing.
	myHashes := h.node.MerkleTree().AllBucketHashes()
	myHashesBytes := make([][]byte, merkle.NumBuckets)
	for i, bh := range myHashes {
		cp := make([]byte, 32)
		copy(cp, bh[:])
		myHashesBytes[i] = cp
	}

	return &repb.HashSyncResponse{
		DivergentBuckets: divergentInt32,
		BucketHashes:     myHashesBytes,
	}, nil
}

// DeltaSync handles Phase 2. The caller lists the buckets it needs entries for.
func (h *Handler) DeltaSync(_ context.Context, req *repb.DeltaSyncRequest) (*repb.DeltaSyncResponse, error) {
	buckets := make([]int, len(req.Buckets))
	for i, b := range req.Buckets {
		buckets[i] = int(b)
	}

	records := h.node.SnapshotBuckets(buckets)
	deltas := make([]*repb.DeltaEntry, 0, len(records))
	for _, rec := range records {
		deltas = append(deltas, encodeEntry(rec))
	}

	return &repb.DeltaSyncResponse{Deltas: deltas}, nil
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
