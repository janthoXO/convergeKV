package grpc_test

import (
	"context"
	"io"
	"net"
	"testing"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	debugpb "github.com/janthoXO/convergeKV/gen/debug"
	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	tgrpc "github.com/janthoXO/convergeKV/internal/transport/grpc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// ── fake coordinator ──────────────────────────────────────────────────────────

type fakeCoord struct {
	putFn    func(ctx context.Context, key, valueJSON string) (hlc.Timestamp, error)
	getFn    func(ctx context.Context, key string) (string, bool, error)
	deleteFn func(ctx context.Context, key string) (hlc.Timestamp, error)
}

func (f *fakeCoord) Put(ctx context.Context, key, valueJSON string) (hlc.Timestamp, error) {
	if f.putFn != nil {
		return f.putFn(ctx, key, valueJSON)
	}
	return hlc.Timestamp{}, nil
}
func (f *fakeCoord) Get(ctx context.Context, key string) (string, bool, error) {
	if f.getFn != nil {
		return f.getFn(ctx, key)
	}
	return `{}`, true, nil
}
func (f *fakeCoord) Delete(ctx context.Context, key string) (hlc.Timestamp, error) {
	if f.deleteFn != nil {
		return f.deleteFn(ctx, key)
	}
	return hlc.Timestamp{}, nil
}
func (f *fakeCoord) Status(_ context.Context) (string, hlc.Timestamp, []string, error) {
	return "test", hlc.Timestamp{}, nil, nil
}

// ── fake debug store ──────────────────────────────────────────────────────────

type fakeStore struct{ entries []struct{ k, f string; e crdt.FieldEntry } }

func (s *fakeStore) IterateAll(_ context.Context, fn func(key, field string, e crdt.FieldEntry) error) error {
	for _, rec := range s.entries {
		if err := fn(rec.k, rec.f, rec.e); err != nil {
			return err
		}
	}
	return nil
}

// ── test harness ──────────────────────────────────────────────────────────────

const bufSize = 1 << 20
const debugToken = "secret"

type clients struct {
	kv    kvpb.KVServiceClient
	fwd   fwdpb.ForwardServiceClient
	debug debugpb.DebugServiceClient
}

func newClients(t *testing.T, coord tgrpc.KV, store tgrpc.DebugStore) (clients, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(tgrpc.NullByteInterceptor))
	kvpb.RegisterKVServiceServer(srv, tgrpc.NewKVHandler(coord))
	fwdpb.RegisterForwardServiceServer(srv, tgrpc.NewForwardHandler(coord))
	if store != nil {
		debugpb.RegisterDebugServiceServer(srv, tgrpc.NewDebugHandler(store, debugToken))
	}
	go srv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() { conn.Close(); srv.GracefulStop() }
	return clients{
		kv:    kvpb.NewKVServiceClient(conn),
		fwd:   fwdpb.NewForwardServiceClient(conn),
		debug: debugpb.NewDebugServiceClient(conn),
	}, cleanup
}

// ── KV handler tests ──────────────────────────────────────────────────────────

func TestKVPutGetDelete(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, nil)
	defer cleanup()

	if _, err := cl.kv.Put(context.Background(), &kvpb.PutRequest{Key: "k", ValueJson: `{"x":1}`}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := cl.kv.Get(context.Background(), &kvpb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() {
		t.Error("expected Found=true")
	}
	if _, err := cl.kv.Delete(context.Background(), &kvpb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestKVNullByteRejected(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, nil)
	defer cleanup()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"Put", func() error {
			_, err := cl.kv.Put(context.Background(), &kvpb.PutRequest{Key: "bad\x00key"})
			return err
		}},
		{"Get", func() error {
			_, err := cl.kv.Get(context.Background(), &kvpb.GetRequest{Key: "bad\x00key"})
			return err
		}},
		{"Delete", func() error {
			_, err := cl.kv.Delete(context.Background(), &kvpb.DeleteRequest{Key: "bad\x00key"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for null-byte key, got %v", err)
			}
		})
	}
}

// ── ForwardHandler tests ──────────────────────────────────────────────────────

func TestForwardNullByteRejected(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, nil)
	defer cleanup()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"ForwardPut", func() error {
			_, err := cl.fwd.ForwardPut(context.Background(), &kvpb.PutRequest{Key: "k\x00"})
			return err
		}},
		{"ForwardGet", func() error {
			_, err := cl.fwd.ForwardGet(context.Background(), &kvpb.GetRequest{Key: "k\x00"})
			return err
		}},
		{"ForwardDelete", func() error {
			_, err := cl.fwd.ForwardDelete(context.Background(), &kvpb.DeleteRequest{Key: "k\x00"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s: expected InvalidArgument, got %v", tc.name, err)
			}
		})
	}
}

func TestForwardValidKey(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, nil)
	defer cleanup()

	if _, err := cl.fwd.ForwardPut(context.Background(), &kvpb.PutRequest{Key: "ok", ValueJson: `{}`}); err != nil {
		t.Fatalf("ForwardPut: %v", err)
	}
}

// ── DebugHandler tests ────────────────────────────────────────────────────────

func scanAll(t *testing.T, cl debugpb.DebugServiceClient, token string) (int, error) {
	t.Helper()
	ctx := context.Background()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	stream, err := cl.ScanAll(ctx, &debugpb.ScanRequest{})
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func TestDebugNoToken(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, &fakeStore{})
	defer cleanup()

	_, err := scanAll(t, cl.debug, "")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated without token, got %v", err)
	}
}

func TestDebugWrongToken(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, &fakeStore{})
	defer cleanup()

	_, err := scanAll(t, cl.debug, "wrong")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated with wrong token, got %v", err)
	}
}

func TestDebugCorrectToken(t *testing.T) {
	store := &fakeStore{entries: []struct{ k, f string; e crdt.FieldEntry }{
		{k: "key1", f: "field1", e: crdt.FieldEntry{}},
	}}
	cl, cleanup := newClients(t, &fakeCoord{}, store)
	defer cleanup()

	count, err := scanAll(t, cl.debug, debugToken)
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry, got %d", count)
	}
}

func TestNullByteInterceptorDoesNotBlockStatus(t *testing.T) {
	cl, cleanup := newClients(t, &fakeCoord{}, nil)
	defer cleanup()

	// Status has no key field — interceptor must not reject it.
	resp, err := cl.kv.Status(context.Background(), &kvpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.GetReplicaId() == "" {
		t.Error("expected non-empty ReplicaId in Status response")
	}
}
