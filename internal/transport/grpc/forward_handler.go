package grpc

import (
	"context"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
)

// ForwardHandler implements fwdpb.ForwardServiceServer.
// It re-uses the same coordinator KV interface so a forwarded write also
// triggers async replication to the remaining co-replicas.
// The server-level NullByteInterceptor covers key validation for both services.
type ForwardHandler struct {
	fwdpb.UnimplementedForwardServiceServer
	coord KV
}

// NewForwardHandler returns a ready-to-register ForwardHandler.
func NewForwardHandler(coord KV) *ForwardHandler { return &ForwardHandler{coord: coord} }

func (h *ForwardHandler) ForwardPut(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ts, err := h.coord.Put(ctx, req.GetKey(), req.GetValueJson())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.PutResponse{Timestamp: reproto.HLCToProto(ts)}, nil
}

func (h *ForwardHandler) ForwardGet(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found, err := h.coord.Get(ctx, req.GetKey())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.GetResponse{ValueJson: value, Found: found}, nil
}

func (h *ForwardHandler) ForwardDelete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ts, err := h.coord.Delete(ctx, req.GetKey())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.DeleteResponse{Timestamp: reproto.HLCToProto(ts)}, nil
}
