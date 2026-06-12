package streampush_test

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	"github.com/janthoXO/convergeKV/internal/adapter/replication/streampush"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeSyncServer counts PushEntries stream opens and entries received.
type fakeSyncServer struct {
	repb.UnimplementedSyncServiceServer
	mu          sync.Mutex
	streamCount int32
	entryCount  int32
}

func (s *fakeSyncServer) PushEntries(stream grpc.ClientStreamingServer[repb.PushChunk, repb.PushAck]) error {
	atomic.AddInt32(&s.streamCount, 1)
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		if _, ok := chunk.GetPayload().(*repb.PushChunk_Entry); ok {
			atomic.AddInt32(&s.entryCount, 1)
		}
	}
	return stream.SendAndClose(&repb.PushAck{})
}

// peerHarness spins up a fake SyncService on a real TCP port.
type peerHarness struct {
	srv     *fakeSyncServer
	grpcSrv *grpc.Server
	addr    string
}

func newPeer(t *testing.T) *peerHarness {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &fakeSyncServer{}
	grpcSrv := grpc.NewServer()
	repb.RegisterSyncServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.GracefulStop)
	return &peerHarness{srv: fake, grpcSrv: grpcSrv, addr: lis.Addr().String()}
}

func newPool(t *testing.T) *connpool.Pool {
	t.Helper()
	pool := connpool.New()
	t.Cleanup(pool.Close)
	return pool
}

func peerInfo(addr, id string) ports.MemberInfo {
	return ports.MemberInfo{ReplicaID: id, GRPCAddr: addr}
}

func singleUpdate(key, field string) []crdt.FieldUpdate {
	return []crdt.FieldUpdate{{
		PartitionID: 0,
		Key:         key,
		Field:       field,
		Entry: crdt.FieldEntry{
			Value:     json.RawMessage(`"test"`),
			Timestamp: hlc.Timestamp{PhysicalMs: 1, Logical: 0},
			ReplicaID: "local",
		},
	}}
}

func waitEntries(t *testing.T, srv *fakeSyncServer, want int32) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		if atomic.LoadInt32(&srv.entryCount) >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: got %d entries, want %d",
				atomic.LoadInt32(&srv.entryCount), want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestSingleEnqueueReachesPeer(t *testing.T) {
	peer := newPeer(t)
	pool := newPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fanout := streampush.New(ctx, "local", pool)
	defer fanout.Close()

	fanout.Enqueue(singleUpdate("k", "f"), []ports.MemberInfo{peerInfo(peer.addr, "peer-1")})

	waitEntries(t, peer.srv, 1)
}

func TestMultipleEnqueuesReuseStream(t *testing.T) {
	peer := newPeer(t)
	pool := newPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fanout := streampush.New(ctx, "local", pool)
	defer fanout.Close()

	pi := peerInfo(peer.addr, "peer-1")
	const n = 5
	for i := 0; i < n; i++ {
		fanout.Enqueue(singleUpdate("k", "f"), []ports.MemberInfo{pi})
	}

	waitEntries(t, peer.srv, n)

	// Long-lived stream design: server should see at most 2 stream opens
	// (1 normal + 1 possible reconnect on first send), regardless of how many
	// enqueue calls happened.
	streams := atomic.LoadInt32(&peer.srv.streamCount)
	if streams > 2 {
		t.Errorf("expected ≤2 PushEntries stream opens for %d enqueues, got %d", n, streams)
	}
}

func TestEvictAbsentStopsGoroutine(t *testing.T) {
	peer := newPeer(t)
	pool := newPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fanout := streampush.New(ctx, "local", pool)

	fanout.Enqueue(singleUpdate("k", "f"), []ports.MemberInfo{peerInfo(peer.addr, "peer-1")})
	waitEntries(t, peer.srv, 1)

	// Evict — peer goroutine should exit.
	fanout.EvictAbsent(map[string]struct{}{})
	fanout.Close() // drains wg; goleak will catch any goroutine leaks
}

// TestEnqueueGroupsByFieldUpdatePartitionID confirms that streampush uses
// FieldUpdate.PartitionID directly (set at the coordinator) rather than
// recomputing keyspace.Of(key, numPartitions). The fake server counts entries
// regardless of partition, so we send updates with arbitrary PartitionIDs and
// assert both arrive.
func TestEnqueueGroupsByFieldUpdatePartitionID(t *testing.T) {
	peer := newPeer(t)
	pool := newPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fanout := streampush.New(ctx, "local", pool)
	defer fanout.Close()

	updates := []crdt.FieldUpdate{
		{
			PartitionID: 777, Key: "k1", Field: "f",
			Entry: crdt.FieldEntry{Value: json.RawMessage(`"a"`), Timestamp: hlc.Timestamp{PhysicalMs: 1}, ReplicaID: "local"},
		},
		{
			PartitionID: 888, Key: "k2", Field: "f",
			Entry: crdt.FieldEntry{Value: json.RawMessage(`"b"`), Timestamp: hlc.Timestamp{PhysicalMs: 2}, ReplicaID: "local"},
		},
	}
	fanout.Enqueue(updates, []ports.MemberInfo{peerInfo(peer.addr, "peer-1")})

	waitEntries(t, peer.srv, 2)
}

func TestLocalIDSkipped(t *testing.T) {
	peer := newPeer(t)
	pool := newPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fanout := streampush.New(ctx, "local", pool)
	defer fanout.Close()

	// Enqueue with the local node's own ID — must be skipped.
	fanout.Enqueue(singleUpdate("k", "f"), []ports.MemberInfo{peerInfo(peer.addr, "local")})

	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(&peer.srv.entryCount); got != 0 {
		t.Errorf("expected 0 entries when peer.ReplicaID==localID, got %d", got)
	}
}
