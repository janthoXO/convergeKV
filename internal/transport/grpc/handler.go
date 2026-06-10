// Package grpc implements the inbound gRPC transport handlers for client-facing
// and internal operations.  A shared null-byte interceptor is applied to all
// inbound RPCs so neither KVService nor ForwardService can receive a poisoned key.
package grpc

import (
	"context"
	"errors"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/core/coordinator"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KV is the domain-typed coordinator interface the KV handler delegates to.
// The handler maps proto requests/responses to and from these signatures.
type KV interface {
	Put(ctx context.Context, key, valueJSON string) (hlc.Timestamp, error)
	Get(ctx context.Context, key string) (value string, found bool, err error)
	Delete(ctx context.Context, key string) (hlc.Timestamp, error)
	Status(ctx context.Context) (replicaID string, ts hlc.Timestamp, peers []string, err error)
}

// KVHandler implements kvpb.KVServiceServer.
type KVHandler struct {
	kvpb.UnimplementedKVServiceServer
	coord KV
}

// NewKVHandler returns a ready-to-register KVHandler.
func NewKVHandler(coord KV) *KVHandler { return &KVHandler{coord: coord} }

func (h *KVHandler) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ts, err := h.coord.Put(ctx, req.GetKey(), req.GetValueJson())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.PutResponse{Timestamp: reproto.HLCToProto(ts)}, nil
}

func (h *KVHandler) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found, err := h.coord.Get(ctx, req.GetKey())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.GetResponse{ValueJson: value, Found: found}, nil
}

func (h *KVHandler) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ts, err := h.coord.Delete(ctx, req.GetKey())
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.DeleteResponse{Timestamp: reproto.HLCToProto(ts)}, nil
}

func (h *KVHandler) Status(ctx context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	replicaID, ts, peers, err := h.coord.Status(ctx)
	if err != nil {
		return nil, mapCoordError(err)
	}
	return &kvpb.StatusResponse{ReplicaId: replicaID, Hlc: reproto.HLCToProto(ts), Peers: peers}, nil
}

// mapCoordError translates domain-level coordinator errors into gRPC status
// errors. ErrNoReplicas (A3) becomes Unavailable so clients retry rather than
// treat a transient membership gap as a permanent failure.
func mapCoordError(err error) error {
	if errors.Is(err, coordinator.ErrNoReplicas) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// NullByteInterceptor is a gRPC unary interceptor that rejects any request
// whose key field contains a null byte. Applied server-wide so both KVService
// and ForwardService receive the same validation.
func NullByteInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	type hasKey interface{ GetKey() string }
	if r, ok := req.(hasKey); ok {
		if err := keyspace.RejectNullBytes(r.GetKey()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return handler(ctx, req)
}
