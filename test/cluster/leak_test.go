package clustertest

import (
	"context"
	"testing"

	"go.uber.org/goleak"

	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// TestNoGoroutineLeaks runs a small cluster through writes, a crash, and a
// graceful leave, stops everything, and verifies no goroutines linger.
func TestNoGoroutineLeaks(t *testing.T) {
	defer goleak.VerifyNone(t,
		// Pebble keeps a process-wide deallocation helper alive by design.
		goleak.IgnoreAnyFunction("github.com/cockroachdb/pebble/v2/internal/manual.processDeallocQueue"),
		// gRPC client connections park a global idleness timer briefly.
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
	)

	h := Start(t, 3)
	ctx := context.Background()
	client := h.Client(0)
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "leak", Value: []byte(`{"v": 1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(ctx, &pb.GetRequest{Key: "leak"}); err != nil {
		t.Fatal(err)
	}

	h.Kill(2)             // crash path
	h.Nodes[1].Stop(true) // graceful path
	h.Nodes[1] = nil
	h.Nodes[0].Stop(false)
	h.Nodes[0] = nil

	for _, c := range h.conns {
		_ = c.Close()
	}
	h.conns = nil
}
