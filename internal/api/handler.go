// Package api implements the gRPC KVService handler for client-facing operations.
package api

import (
	"context"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/node"
)

// Handler implements kvpb.KVServiceServer.
type Handler struct {
	kvpb.UnimplementedKVServiceServer
	coord *coordinator.Coordinator
	node  *node.Node   // used only for Status
	peers []string
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(coord *coordinator.Coordinator, n *node.Node, peers []string) *Handler {
	return &Handler{coord: coord, node: n, peers: peers}
}

// Put routes a put request through the coordinator.
func (h *Handler) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	return h.coord.Put(ctx, req)
}

// Get routes a get request through the coordinator.
func (h *Handler) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	return h.coord.Get(ctx, req)
}

// Delete routes a delete request through the coordinator.
func (h *Handler) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	return h.coord.Delete(ctx, req)
}

// Status returns the node's replica ID, current HLC, and peer list.
func (h *Handler) Status(_ context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	ts := h.node.HLCNow()
	return &kvpb.StatusResponse{
		ReplicaId: h.node.ReplicaID(),
		Hlc:       &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
		Peers:     h.peers,
	}, nil
}
