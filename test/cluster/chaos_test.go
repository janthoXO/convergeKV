package clustertest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/placement"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// chaosClients hands out KV clients by node index, re-dialing when a node
// restarted on a new port. Safe for concurrent use.
type chaosClients struct {
	mu    sync.Mutex
	h     *Harness
	conns map[int]*grpc.ClientConn
	addrs map[int]string
}

func newChaosClients(h *Harness) *chaosClients {
	return &chaosClients{h: h, conns: make(map[int]*grpc.ClientConn), addrs: make(map[int]string)}
}

// get returns a client for node i, or nil if the node is down.
func (c *chaosClients) get(i int) pb.KVClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	nd := c.h.get(i)
	if nd == nil {
		return nil
	}
	addr := nd.ClientAddr()
	if conn, ok := c.conns[i]; ok && c.addrs[i] == addr {
		return pb.NewKVClient(conn)
	}
	if conn, ok := c.conns[i]; ok {
		_ = conn.Close()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	c.conns[i], c.addrs[i] = conn, addr
	return pb.NewKVClient(conn)
}

func (c *chaosClients) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

// TestChaos runs randomized load while crashing and restarting nodes, then
// checks the section-5 invariants: every owner of every key converges to a
// byte-identical document (or identical absence). Duration defaults to 30s;
// set CONVERGEKV_CHAOS_DURATION (e.g. "1h") for the full acceptance run.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos run skipped in -short mode")
	}
	duration := 30 * time.Second
	if v := os.Getenv("CONVERGEKV_CHAOS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("CONVERGEKV_CHAOS_DURATION: %v", err)
		}
		duration = d
	}

	const n = 5
	h := Start(t, n)
	ctx := context.Background()
	clients := newChaosClients(h)
	defer clients.close()

	const keyCount = 24
	key := func(i int) string { return fmt.Sprintf("chaos-%d", i) }

	var ops, opErrors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Load: writers/readers hammering random keys through random running
	// nodes. Errors are tolerated (nodes die underneath them) but counted;
	// the healthy-path tests guarantee zero, here we require convergence.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				client := clients.get(rng.Intn(n))
				if client == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				k := key(rng.Intn(keyCount))
				cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				var err error
				switch rng.Intn(10) {
				case 0: // occasional delete
					_, err = client.Delete(cctx, &pb.DeleteRequest{Key: k})
				case 1, 2, 3, 4:
					_, err = client.Put(cctx, &pb.PutRequest{
						Key:      k,
						Value: fmt.Appendf(nil, `{"w": %d, "v": %d, "nested": {"deep": [%d]}}`, seed, rng.Int63(), rng.Intn(100)),
					})
				default:
					_, err = client.Get(cctx, &pb.GetRequest{Key: k})
				}
				cancel()
				ops.Add(1)
				if err != nil {
					opErrors.Add(1)
				}
				time.Sleep(time.Duration(rng.Intn(8)) * time.Millisecond)
			}
		}(int64(w))
	}

	// Chaos: random crash/restart churn, always keeping a majority running.
	rng := rand.New(rand.NewSource(99))
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(1500+rng.Intn(1500)) * time.Millisecond)
		running, down := h.partition()
		switch {
		case len(down) > 0 && (len(running) <= 3 || rng.Intn(2) == 0):
			i := down[rng.Intn(len(down))]
			t.Logf("chaos: restarting node %d", i)
			h.Restart(i)
		case len(running) > 3:
			i := running[rng.Intn(len(running))]
			t.Logf("chaos: killing node %d", i)
			h.Kill(i)
		}
	}

	// Heal: bring everything back, require full convergence, stop load.
	for _, i := range h.downNodes() {
		h.Restart(i)
	}
	h.WaitConverged(n)
	close(stop)
	wg.Wait()
	t.Logf("chaos done: %d ops, %d transient errors", ops.Load(), opErrors.Load())
	if ops.Load() == 0 {
		t.Fatal("load generator produced no operations")
	}

	// Invariant 1/5: all owners byte-identical per key (or identically
	// absent) once anti-entropy settles.
	for i := 0; i < keyCount; i++ {
		k := key(i)
		pid := placement.Partition([]byte(k), Partitions)
		converged := func() bool {
			owners, _ := h.Owners(k)
			if len(owners) == 0 {
				return false
			}
			var want []byte
			first := true
			for _, oi := range owners {
				doc, err := h.Nodes[oi].Store.GetDocument(pid, []byte(k))
				if err != nil {
					return false
				}
				var enc []byte
				if doc != nil {
					enc = doc.Canonical()
				}
				if first {
					want, first = enc, false
				} else if !bytes.Equal(enc, want) {
					return false
				}
			}
			return true
		}
		// Generous budget: under -race, AE rounds and repairs run ~10x
		// slower; the bound that matters is "settles at all".
		deadline := time.Now().Add(90 * time.Second)
		for !converged() {
			if time.Now().After(deadline) {
				h.dumpKeyState(t, k, pid)
				t.Fatalf("post-chaos convergence on %s timed out", k)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// dumpKeyState prints each owner's view of a key for divergence debugging.
func (h *Harness) dumpKeyState(t *testing.T, k string, pid uint16) {
	owners, _ := h.Owners(k)
	for _, oi := range owners {
		nd := h.Nodes[oi]
		doc, err := nd.Store.GetDocument(pid, []byte(k))
		counter, _ := nd.Store.GCCounter(pid, []byte(k))
		if doc == nil {
			t.Logf("owner %d (%x): doc=ABSENT gcCounter=%d err=%v", oi, nd.ID[:4], counter, err)
			continue
		}
		var regs []string
		for f, rs := range doc.Fields {
			for _, r := range rs {
				regs = append(regs, fmt.Sprintf("%s=(%x,%d,hlc=%d,len=%d)", f, r.Dot.Actor[:3], r.Dot.Seq, r.HLC, len(r.Value)))
			}
		}
		t.Logf("owner %d (%x): hash=%x fields=%d ctxVV=%v cloud=%d gc=%d clean=%d regs=%v",
			oi, nd.ID[:4], doc.Canonical()[:0:0], len(doc.Fields), summarizeVV(doc),
			len(doc.Context.Cloud), counter, nd.AE.CleanRounds(pid), regs)
		t.Logf("owner %d canonical sha: %x", oi, sha256.Sum256(doc.Canonical()))
	}
}

func summarizeVV(doc *crdt.Document) map[string]uint64 {
	out := map[string]uint64{}
	for a, s := range doc.Context.VV {
		out[fmt.Sprintf("%x", a[:4])] = s
	}
	return out
}
