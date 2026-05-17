package syncer

import (
	"context"
	"log"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/node"
)

// Handler implements repb.SyncServiceServer.
type Handler struct {
	repb.UnimplementedSyncServiceServer
	node      *node.Node
	ibltState *IBLTState
}

// NewHandler returns a Handler ready to register with a gRPC server.
func NewHandler(n *node.Node, ibltState *IBLTState) *Handler {
	return &Handler{node: n, ibltState: ibltState}
}

// IBLTExchange is the responder side of Round 1.
// Receives the initiator's IBLT, XORs with local, decodes, returns the diff.
func (h *Handler) IBLTExchange(_ context.Context, req *repb.IBLTExchangeRequest) (*repb.IBLTExchangeResponse, error) {
	// Decode the initiator's IBLT.
	remoteIBLT, err := IBLTFromEncoded(req.GetIbltData())
	if err != nil {
		log.Printf("[syncer/handler] IBLTExchange decode remote IBLT from %s: %v", req.GetReplicaId(), err)
		return &repb.IBLTExchangeResponse{Decodable: false}, nil
	}

	// Compute the symmetric difference IBLT.
	localSnap := h.ibltState.Snapshot()
	diff := localSnap.SubtractUnsafe(remoteIBLT)

	onlyLocal, onlyRemote, ok := diff.Decode()
	if !ok {
		log.Printf("[syncer/handler] IBLT diff too large from %s — signalling fallback", req.GetReplicaId())
		return &repb.IBLTExchangeResponse{Decodable: false}, nil
	}

	// onlyLocal: items B (us) has that A doesn't → send full entries to A.
	// onlyRemote: items A has that B (us) doesn't → tell A to send them to us.

	nodeSnap := h.node.Snapshot()
	itemsForInitiator := make([]*repb.DeltaEntry, 0, len(onlyLocal))
	for _, itemBytes := range onlyLocal {
		key, field, replicaID, physMs, logical, deleted, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		for _, r := range nodeSnap {
			if r.Key == key && r.Field == field &&
				r.Entry.Timestamp.PhysicalMs == physMs &&
				r.Entry.Timestamp.Logical == logical &&
				r.Entry.ReplicaID == replicaID &&
				r.Entry.Deleted == deleted {
				itemsForInitiator = append(itemsForInitiator, &repb.DeltaEntry{
					Key:       r.Key,
					Field:     r.Field,
					ValueJson: r.Entry.Value,
					Timestamp: &kvpb.HLCTimestamp{
						PhysicalMs: r.Entry.Timestamp.PhysicalMs,
						Logical:    r.Entry.Timestamp.Logical,
					},
					ReplicaId: r.Entry.ReplicaID,
					Deleted:   r.Entry.Deleted,
				})
				break
			}
		}
	}

	iNeed := make([]*repb.ItemIdentifier, 0, len(onlyRemote))
	for _, itemBytes := range onlyRemote {
		key, field, replicaID, physMs, logical, _, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		iNeed = append(iNeed, &repb.ItemIdentifier{
			Key:        key,
			Field:      field,
			PhysicalMs: physMs,
			Logical:    logical,
			ReplicaId:  replicaID,
		})
	}

	return &repb.IBLTExchangeResponse{
		Decodable:         true,
		ItemsForInitiator: itemsForInitiator,
		INeed:             iNeed,
	}, nil
}

// PushEntries is called by a peer to deliver entries it has written.
// Each entry is applied via CRDT merge; the IBLT is updated by ApplyDelta
// (which calls ibltState.InsertEntry/RemoveEntry through the node's ibltState field).
func (h *Handler) PushEntries(_ context.Context, req *repb.PushEntriesRequest) (*repb.PushEntriesResponse, error) {
	applied := int32(0)
	for _, d := range req.GetEntries() {
		ts := hlc.Timestamp{
			PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
			Logical:    d.GetTimestamp().GetLogical(),
		}
		entry := crdt.FieldEntry{
			Value:     d.GetValueJson(),
			Timestamp: ts,
			ReplicaID: d.GetReplicaId(),
			Deleted:   d.GetDeleted(),
		}
		changed, err := h.node.ApplyDelta(d.GetKey(), d.GetField(), entry)
		if err != nil {
			log.Printf("[syncer/handler] PushEntries apply key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
			continue
		}
		if changed {
			applied++
		}
	}
	return &repb.PushEntriesResponse{Applied: applied}, nil
}

// FullStateSync is the full-state fallback handler.
// Applies the initiator's full state, then returns the local full state.
func (h *Handler) FullStateSync(_ context.Context, req *repb.FullStateSyncRequest) (*repb.FullStateSyncResponse, error) {
	// Apply incoming entries.
	for _, d := range req.GetEntries() {
		ts := hlc.Timestamp{
			PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
			Logical:    d.GetTimestamp().GetLogical(),
		}
		entry := crdt.FieldEntry{
			Value:     d.GetValueJson(),
			Timestamp: ts,
			ReplicaID: d.GetReplicaId(),
			Deleted:   d.GetDeleted(),
		}
		if _, err := h.node.ApplyDelta(d.GetKey(), d.GetField(), entry); err != nil {
			log.Printf("[syncer/handler] FullStateSync apply key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
		}
	}

	// Return our full state.
	localRecords := h.node.Snapshot()
	resp := make([]*repb.DeltaEntry, 0, len(localRecords))
	for _, r := range localRecords {
		resp = append(resp, &repb.DeltaEntry{
			Key:       r.Key,
			Field:     r.Field,
			ValueJson: r.Entry.Value,
			Timestamp: &kvpb.HLCTimestamp{
				PhysicalMs: r.Entry.Timestamp.PhysicalMs,
				Logical:    r.Entry.Timestamp.Logical,
			},
			ReplicaId: r.Entry.ReplicaID,
			Deleted:   r.Entry.Deleted,
		})
	}

	log.Printf("[syncer/handler] FullStateSync from %s: applied %d, returning %d", req.GetReplicaId(), len(req.GetEntries()), len(resp))
	return &repb.FullStateSyncResponse{Entries: resp}, nil
}
