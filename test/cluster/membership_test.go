package clustertest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/placement"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// addNode joins one more node to the running cluster.
func (h *Harness) addNode(t *testing.T) *node.Node {
	t.Helper()
	seeds := []string{h.firstRunning().Cluster().GossipAddr()}
	cfg := nodeConfig(t, seeds)
	nd, err := node.Start(cfg, nil)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	h.Nodes = append(h.Nodes, nd)
	h.cfgs = append(h.cfgs, cfg)
	return nd
}

func TestScaleOutUnderLoadZeroFailures(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()
	client := h.Client(0)

	// Seed some data.
	for i := 0; i < 32; i++ {
		key := fmt.Sprintf("scale-key-%d", i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: fmt.Appendf(nil, `{"i": %d}`, i)}); err != nil {
			t.Fatal(err)
		}
	}

	// Continuous load while scaling 3 -> 5.
	var failures atomic.Int64
	var ops atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("scale-key-%d", i%32)
			if i%3 == 0 {
				if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: fmt.Appendf(nil, `{"i": %d}`, i)}); err != nil {
					t.Logf("put %s failed: %v", key, err)
					failures.Add(1)
				}
			} else {
				if _, err := client.Get(ctx, &pb.GetRequest{Key: key}); err != nil {
					t.Logf("get %s failed: %v", key, err)
					failures.Add(1)
				}
			}
			ops.Add(1)
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	h.addNode(t)
	h.addNode(t)
	h.WaitConverged(5)

	close(stop)
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d of %d ops failed during scale-out", failures.Load(), ops.Load())
	}
	t.Logf("%d ops, zero failures; all partitions fully active on 5 nodes", ops.Load())

	// Every key still readable, all owners byte-identical (deltas fanned to
	// a stale owner set during the scale-out are healed by AE rounds).
	for i := 0; i < 32; i++ {
		h.WaitOwnersConverged(fmt.Sprintf("scale-key-%d", i), 10*time.Second)
	}
}

func TestPermanentCrashRestoresRFAfterGrace(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()
	client := h.Client(0)

	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("rf-key-%d", i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
			t.Fatal(err)
		}
		h.WaitOwnersConverged(key, time.Second)
	}

	h.Kill(3) // permanent: grace period (1s in the harness) expires

	// After grace, the survivor set must restore RF=3 active owners per
	// partition via bootstrap transfer.
	h.waitFor(15*time.Second, "RF restoration after grace", func() bool {
		for _, nd := range h.Nodes {
			if nd == nil {
				continue
			}
			v := nd.View()
			for pid := uint16(0); pid < Partitions; pid++ {
				if len(v.ReadSet(pid)) != 3 {
					return false
				}
			}
		}
		return true
	})
	for i := 0; i < 16; i++ {
		h.WaitOwnersConverged(fmt.Sprintf("rf-key-%d", i), 3*time.Second)
	}
}

func TestRestartWithinGraceNoTransfer(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()
	client := h.Client(0)

	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("restart-key-%d", i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
			t.Fatal(err)
		}
		h.WaitOwnersConverged(key, time.Second)
	}

	// Crash and restart quickly (well within the 1s grace? the restart
	// itself takes time — what matters is that the node comes back with its
	// data, so no transfer is needed regardless).
	h.Kill(3)
	nd := h.Restart(3)
	h.WaitConverged(4)

	// The node had its data on disk: it must rejoin active via AE catch-up,
	// without a single bootstrap transfer.
	if got := nd.Transfer().Started(); got != 0 {
		t.Fatalf("restart triggered %d bootstrap transfers, want 0", got)
	}
	for i := 0; i < 16; i++ {
		h.WaitOwnersConverged(fmt.Sprintf("restart-key-%d", i), 5*time.Second)
	}
}

func TestGracefulLeaveKeepsServing(t *testing.T) {
	h := Start(t, 5)
	ctx := context.Background()
	client := h.Client(0)

	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("leave-key-%d", i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": "x"}`)}); err != nil {
			t.Fatal(err)
		}
		h.WaitOwnersConverged(key, time.Second)
	}

	// Planned leave of node 4: drain, successors go active, then exit.
	h.Nodes[4].Stop(true)
	h.Nodes[4] = nil
	h.WaitConverged(4)

	// All data still served with full RF.
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("leave-key-%d", i)
		got, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil || !got.GetFound() {
			t.Fatalf("key %s lost after planned leave: %v", key, err)
		}
		pid := placement.Partition([]byte(key), Partitions)
		if rs := h.firstRunning().View().ReadSet(pid); len(rs) != 3 {
			t.Fatalf("partition %d has %d active owners after leave", pid, len(rs))
		}
	}
}
