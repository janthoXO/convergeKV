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
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Handler implements repb.SyncServiceServer.
type Handler struct {
	repb.UnimplementedSyncServiceServer
	node      *node.Node
	ibltState *IBLTState
	store     *storage.Store
}

// NewHandler returns a Handler ready to register with a gRPC server.
func NewHandler(n *node.Node, ibltState *IBLTState, store *storage.Store) *Handler {
	return &Handler{node: n, ibltState: ibltState, store: store}
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

	// onlyLocal: items we have that the initiator doesn't → send full entries.
	// onlyRemote: items the initiator has that we don't → tell it to send them.

	itemsForInitiator := make([]*repb.DeltaEntry, 0, len(onlyLocal))
	for _, itemBytes := range onlyLocal {
		key, field, replicaID, physMs, logical, deleted, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := h.store.GetField(key, field)
		if err != nil || !found {
			continue
		}
		// Verify it's the exact version the IBLT encodes (timestamps must match).
		if entry.Timestamp.PhysicalMs != physMs ||
			entry.Timestamp.Logical != logical ||
			entry.ReplicaID != replicaID ||
			entry.Deleted != deleted {
			continue
		}
		itemsForInitiator = append(itemsForInitiator, entryToProto(key, field, entry))
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

// FullStateSync is the full-state fallback handler (bidirectional streaming).
// Concurrently receives and applies the initiator's entries while streaming
// the local full state back, so neither side buffers the whole dataset.
func (h *Handler) FullStateSync(stream repb.SyncService_FullStateSyncServer) error {
	// Recv goroutine: apply every entry the initiator sends.
	recvDone := make(chan error, 1)

	var initiatorID string
	applied := 0
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				recvDone <- nil
				return
			}

			if err != nil {
				recvDone <- err
				return
			}

			if hdr := msg.GetHeader(); hdr != nil {
				initiatorID = hdr.GetReplicaId()
				continue
			}

			if d := msg.GetEntry(); d != nil {
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
				} else {
					applied++
				}
			}
		}
	}()

	// Stream local full state back while the recv goroutine runs.
	sent := 0
	if err := h.store.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
		sent++
		return stream.Send(&repb.FullStateSyncMessage{
			Payload: &repb.FullStateSyncMessage_Entry{Entry: entryToProto(key, field, entry)},
		})
	}); err != nil {
		log.Printf("[syncer/handler] FullStateSync iterate: %v", err)
	}

	if err := <-recvDone; err != nil {
		return err
	}

	log.Printf("[syncer/handler] FullStateSync from %s: applied %d, sent %d", initiatorID, applied, sent)
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
