package replication

import (
	"context"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/ring"
)

// Handler implements repb.ReplicationServiceServer.
type Handler struct {
	repb.UnimplementedReplicationServiceServer
	node *node.Node
	ring *ring.Ring
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node, r *ring.Ring) *Handler {
	return &Handler{node: n, ring: r}
}

// HashSync handles Phase 1. The caller sends a sparse map of partition hashes
// (only shared partitions); we reply with which of those differ and our own
// hashes for the same partitions.
func (h *Handler) HashSync(_ context.Context, req *repb.HashSyncRequest) (*repb.HashSyncResponse, error) {
	peerHashes := req.GetPartitionHashes() // map[int32][]byte

	myHashesForSender := make(map[int32][]byte, len(peerHashes))
	var divergent []int32

	for partInt32, peerHashBytes := range peerHashes {
		partition := int(partInt32)
		if partition < 0 || partition >= merkle.NumPartitions {
			continue
		}

		myHash := h.node.MerkleTree().PartitionHash(partition)

		// Record our hash for this partition so the sender can compute
		// what IT is missing from us.
		myHashBytes := make([]byte, 32)
		copy(myHashBytes, myHash[:])
		myHashesForSender[partInt32] = myHashBytes

		// Compare
		if len(peerHashBytes) != 32 {
			divergent = append(divergent, partInt32)
			continue
		}
		var peerHash merkle.Hash
		copy(peerHash[:], peerHashBytes)
		if myHash != peerHash {
			divergent = append(divergent, partInt32)
		}
	}

	return &repb.HashSyncResponse{
		DivergentPartitions: divergent,
		PartitionHashes:     myHashesForSender,
	}, nil
}

// DeltaSync handles Phase 2. The caller lists the partitions it needs entries for.
// Only entries that the requester is responsible for are included in the response.
func (h *Handler) DeltaSync(_ context.Context, req *repb.DeltaSyncRequest) (*repb.DeltaSyncResponse, error) {
	partitionSet := make(map[int]struct{}, len(req.GetPartitions()))
	for _, p := range req.GetPartitions() {
		partitionSet[int(p)] = struct{}{}
	}

	records := h.node.SnapshotPartitions(partitionSet)
	deltas := make([]*repb.DeltaEntry, 0, len(records))
	requesterID := req.GetRequesterId()

	for _, rec := range records {
		// Only send entries that the requester is supposed to hold.
		// Use partition ownership (faster than per-key ring lookup).
		if requesterID != "" && h.ring != nil {
			partition := merkle.PartitionIndex(rec.Key)
			if !h.ring.OwnsPartition(partition, requesterID) {
				continue
			}
		}
		deltas = append(deltas, encodeEntry(rec))
	}

	return &repb.DeltaSyncResponse{Deltas: deltas}, nil
}

// encodeEntry converts a KeyFieldEntryTuple into a protobuf DeltaEntry for the wire.
func encodeEntry(rec node.KeyFieldEntryTuple) *repb.DeltaEntry {
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
