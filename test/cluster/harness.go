// Package clustertest provides the in-process multi-node harness used by the
// integration, chaos, and benchmark suites.
package clustertest

import (
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/janthoXO/convergeKV/internal/config"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/placement"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// Partitions is kept small in tests: faster convergence, same code paths.
const Partitions = 16

type Harness struct {
	t     *testing.T
	Nodes []*node.Node
	cfgs  []config.Config
	conns []*grpc.ClientConn
}

func fastMemberlist() *memberlist.Config {
	mlc := memberlist.DefaultLocalConfig()
	mlc.BindAddr = "127.0.0.1"
	mlc.BindPort = 0
	mlc.ProbeInterval = 100 * time.Millisecond
	mlc.ProbeTimeout = 50 * time.Millisecond
	mlc.SuspicionMult = 2
	mlc.GossipInterval = 50 * time.Millisecond
	mlc.PushPullInterval = time.Second
	return mlc
}

func nodeConfig(t *testing.T, seeds []string) config.Config {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ClientAddr = "127.0.0.1:0"
	cfg.NodeAddr = "127.0.0.1:0"
	cfg.GossipAddr = "127.0.0.1:0"
	cfg.Partitions = Partitions
	cfg.Seeds = seeds
	cfg.CrashGracePeriod = time.Second
	// AE is the only repair mechanism for deltas that went to a stale owner
	// set during membership changes; run it hot in tests.
	cfg.AntiEntropyInterval = 500 * time.Millisecond
	cfg.MemberlistConfig = fastMemberlist()
	return cfg
}

// Start brings up an n-node cluster and waits until membership and placement
// have fully converged.
func Start(t *testing.T, n int) *Harness {
	t.Helper()
	h := &Harness{t: t}
	var seeds []string
	for i := 0; i < n; i++ {
		cfg := nodeConfig(t, seeds)
		nd, err := node.Start(cfg, nil)
		if err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
		h.Nodes = append(h.Nodes, nd)
		h.cfgs = append(h.cfgs, cfg)
		if i == 0 {
			seeds = []string{nd.Cluster().GossipAddr()}
		}
	}
	t.Cleanup(func() {
		for _, nd := range h.Nodes {
			if nd != nil {
				nd.Stop(false)
			}
		}
		for _, c := range h.conns {
			_ = c.Close()
		}
	})
	h.WaitConverged(n)
	return h
}

// WaitConverged blocks until every running node sees `alive` members and a
// full active read set for every partition.
func (h *Harness) WaitConverged(alive int) {
	h.t.Helper()
	wantOwners := min(placement.RF, alive)
	h.waitFor(20*time.Second, "cluster convergence", func() bool {
		for _, nd := range h.Nodes {
			if nd == nil {
				continue
			}
			if len(nd.Cluster().Members()) != alive {
				return false
			}
			v := nd.View()
			for pid := uint16(0); pid < Partitions; pid++ {
				if len(v.ReadSet(pid)) != wantOwners {
					return false
				}
			}
		}
		return true
	})
}

func (h *Harness) waitFor(timeout time.Duration, what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

// Client returns a KV client connected to node i.
func (h *Harness) Client(i int) pb.KVClient {
	h.t.Helper()
	conn, err := grpc.NewClient(h.Nodes[i].ClientAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatal(err)
	}
	h.conns = append(h.conns, conn)
	return pb.NewKVClient(conn)
}

// Owners returns the indexes of the nodes owning the key, and the partition.
func (h *Harness) Owners(key string) ([]int, uint16) {
	pid := placement.Partition([]byte(key), Partitions)
	v := h.firstRunning().View()
	var idx []int
	for _, o := range v.Owners(pid) {
		for i, nd := range h.Nodes {
			if nd != nil && nd.ID == o.ID {
				idx = append(idx, i)
			}
		}
	}
	return idx, pid
}

// NonOwner returns the index of some running node that does not own the key.
func (h *Harness) NonOwner(key string) int {
	owners, _ := h.Owners(key)
	for i, nd := range h.Nodes {
		if nd == nil {
			continue
		}
		owned := false
		for _, o := range owners {
			if o == i {
				owned = true
			}
		}
		if !owned {
			return i
		}
	}
	h.t.Fatal("every running node owns the key; use a larger cluster")
	return -1
}

// Kill crashes node i (no leave broadcast) and forgets it.
func (h *Harness) Kill(i int) {
	h.Nodes[i].Stop(false)
	h.Nodes[i] = nil
}

// Restart starts node i again on its original data dir, joining via any
// running peer.
func (h *Harness) Restart(i int) *node.Node {
	h.t.Helper()
	cfg := h.cfgs[i]
	cfg.Seeds = []string{h.firstRunning().Cluster().GossipAddr()}
	nd, err := node.Start(cfg, nil)
	if err != nil {
		h.t.Fatalf("restart node %d: %v", i, err)
	}
	h.Nodes[i] = nd
	return nd
}

func (h *Harness) firstRunning() *node.Node {
	for _, nd := range h.Nodes {
		if nd != nil {
			return nd
		}
	}
	h.t.Fatal("no running nodes")
	return nil
}

// WaitOwnersConverged polls until every owner of the key stores a
// byte-identical document (fields AND causal context).
func (h *Harness) WaitOwnersConverged(key string, within time.Duration) {
	h.t.Helper()
	owners, pid := h.Owners(key)
	h.waitFor(within, "owner convergence on "+key, func() bool {
		var want []byte
		for _, i := range owners {
			doc, err := h.Nodes[i].Store.GetDocument(pid, []byte(key))
			if err != nil || doc == nil {
				return false
			}
			enc := doc.Canonical()
			if want == nil {
				want = enc
			} else if string(enc) != string(want) {
				return false
			}
		}
		return want != nil
	})
}
