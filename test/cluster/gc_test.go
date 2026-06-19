package clustertest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/placement"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// countDocs scans every partition of a node and returns how many documents
// (live or residual) it stores.
func countDocs(t *testing.T, nd *node.Node) int {
	t.Helper()
	n := 0
	for pid := uint16(0); pid < Partitions; pid++ {
		if err := nd.Store.ScanPartition(pid, func([]byte, *crdt.Document) error {
			n++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func TestResidualContextsPurged(t *testing.T) {
	h := Start(t, 3)
	ctx := context.Background()
	client := h.Client(0)

	const keys = 20
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("purge-%d", i)
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i++ {
		h.WaitOwnersConverged(fmt.Sprintf("purge-%d", i), 2*time.Second)
	}
	for i := 0; i < keys; i++ {
		if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: fmt.Sprintf("purge-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	// Background AE rounds certify the residuals (2 clean rounds per
	// partition) and then reap them on every owner.
	h.waitFor(30*time.Second, "residual purge on all nodes", func() bool {
		for _, nd := range h.Nodes {
			if countDocs(t, nd) != 0 {
				return false
			}
		}
		return true
	})

	// GC counters go with the documents.
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("purge-%d", i)
		pid := placement.Partition([]byte(key), Partitions)
		for _, nd := range h.Nodes {
			if c, err := nd.Store.GCCounter(pid, []byte(key)); err != nil || c != 0 {
				t.Fatalf("leftover gc counter %d for %s (err %v)", c, key, err)
			}
		}
	}
}

// A replica that was offline through a delete AND the subsequent GC must not
// resurrect the key when it returns.
func TestNoResurrectionAfterOfflineReplicaReturns(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const key = "lazarus"
	if _, err := h.Client(0).Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": "alive"}`)}); err != nil {
		t.Fatal(err)
	}
	h.WaitOwnersConverged(key, 2*time.Second)

	owners, pid := h.Owners(key)
	victim := owners[0]
	if h.Nodes[victim] == nil {
		t.Fatal("victim already down")
	}
	h.Kill(victim)

	// Delete while the owner is offline, via a surviving node (retry while
	// stale views still route to the dead applier).
	survivor := -1
	for i, nd := range h.Nodes {
		if nd != nil {
			survivor = i
			break
		}
	}
	client := h.Client(survivor)
	h.waitFor(5*time.Second, "delete via survivor", func() bool {
		_, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
		return err == nil
	})

	// GC must stay frozen while the victim is a dead-in-grace owner, and
	// proceed once the grace period (1s) prunes it. Wait for the purge on
	// all running nodes.
	h.waitFor(30*time.Second, "residual purge after grace", func() bool {
		for _, nd := range h.Nodes {
			if nd == nil {
				continue
			}
			if doc, err := nd.Store.GetDocument(pid, []byte(key)); err != nil || doc != nil {
				return false
			}
		}
		return true
	})

	// Make sure the victim's liveness lease is well past the grace period,
	// then bring it back: it must wipe and re-bootstrap.
	time.Sleep(1500 * time.Millisecond)
	nd := h.Restart(victim)
	h.WaitConverged(4)

	h.waitFor(15*time.Second, "returned replica must not hold the key", func() bool {
		doc, err := nd.Store.GetDocument(pid, []byte(key))
		return err == nil && doc == nil
	})
	for i := range h.Nodes {
		got, err := h.Client(i).Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetFound() {
			t.Fatalf("RESURRECTION: node %d serves the deleted key", i)
		}
	}
}

func TestDeadActorsRetiredFromContexts(t *testing.T) {
	h := Start(t, 4)
	ctx := context.Background()

	const keys = 12
	// Write via every node so multiple actors mint dots.
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("retire-%d", i)
		if _, err := h.Client(i%4).Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i++ {
		h.WaitOwnersConverged(fmt.Sprintf("retire-%d", i), 2*time.Second)
	}

	deadActor := crdt.ActorID(h.Nodes[3].ID)
	h.Kill(3)
	time.Sleep(1200 * time.Millisecond) // past grace: actor becomes retirable

	// Supersede the dead actor's registers so retirement can drop its VV
	// entries (live registers pin them).
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("retire-%d", i)
		if _, err := h.Client(0).Put(ctx, &pb.PutRequest{Key: key, Value: []byte(`{"v": 2}`)}); err != nil {
			t.Fatal(err)
		}
	}

	h.waitFor(30*time.Second, "dead actor retired from all contexts", func() bool {
		for _, nd := range h.Nodes {
			if nd == nil {
				continue
			}
			for pid := uint16(0); pid < Partitions; pid++ {
				stale := false
				err := nd.Store.ScanPartition(pid, func(key []byte, doc *crdt.Document) error {
					if _, ok := doc.Context.VV[deadActor]; ok {
						// Live registers by the dead actor legitimately pin
						// the entry; everything else must be gone.
						for _, regs := range doc.Fields {
							for _, r := range regs {
								if r.Dot.Actor == deadActor {
									return nil
								}
							}
						}
						stale = true
					}
					for d := range doc.Context.Cloud {
						if d.Actor == deadActor {
							stale = true
						}
					}
					return nil
				})
				if err != nil || stale {
					return false
				}
			}
		}
		return true
	})
}
