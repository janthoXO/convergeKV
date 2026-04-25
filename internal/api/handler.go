// Package api implements the gRPC KVService handler for client-facing operations.
package api

import (
	"context"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/replication"
)

// Handler implements kvpb.KVServiceServer.
type Handler struct {
	kvpb.UnimplementedKVServiceServer
	node   *node.Node
	peers  []string
	causal *replication.CausalContext
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node, peers []string, causal *replication.CausalContext) *Handler {
	return &Handler{node: n, peers: peers, causal: causal}
}

// Put writes a JSON object to the given key on this node.
func (h *Handler) Put(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ts, err := h.node.Put(req.GetKey(), req.GetValueJson())
	if err != nil {
		return nil, err
	}
	// Record own write in causal context.
	h.causal.Update(h.node.ReplicaID(), ts)
	return &kvpb.PutResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, nil
}

// Get returns the current merged JSON value for the given key.
func (h *Handler) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	v, found := h.node.Get(req.GetKey())
	return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
}

// Delete tombstones all current fields of the given key.
func (h *Handler) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ts, err := h.node.Delete(req.GetKey())
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{
		Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
	}, nil
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
