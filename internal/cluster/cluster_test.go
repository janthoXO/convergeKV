package cluster

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func testConfig(id byte, p uint16, seeds []string) Config {
	mlc := memberlist.DefaultLocalConfig()
	mlc.BindAddr = "127.0.0.1"
	mlc.BindPort = 0 // pick a free port
	// Aggressive timing for fast failure detection in tests.
	mlc.ProbeInterval = 100 * time.Millisecond
	mlc.ProbeTimeout = 50 * time.Millisecond
	mlc.SuspicionMult = 2
	mlc.GossipInterval = 50 * time.Millisecond
	mlc.PushPullInterval = 1 * time.Second
	return Config{
		NodeID:     [16]byte{id},
		Partitions: p,
		Seeds:      seeds,
		Memberlist: mlc,
		Logger:     slog.Default(),
	}
}

func gossipAddr(c *Cluster) string {
	return c.ml.LocalNode().Address()
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestFiveNodeConvergence(t *testing.T) {
	const n = 5
	var nodes []*Cluster
	var seed string
	start := time.Now()
	for i := 0; i < n; i++ {
		var seeds []string
		if seed != "" {
			seeds = []string{seed}
		}
		c, err := Join(testConfig(byte(i+1), 256, seeds))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Shutdown() //nolint:errcheck
		nodes = append(nodes, c)
		if seed == "" {
			seed = gossipAddr(c)
		}
	}
	for _, c := range nodes {
		waitFor(t, 2*time.Second, "5-node convergence", func() bool {
			return len(c.Members()) == n
		})
	}
	t.Logf("converged in %v", time.Since(start))
}

func TestPartitionCountMismatchRejected(t *testing.T) {
	seed, err := Join(testConfig(1, 256, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Shutdown() //nolint:errcheck

	if bad, err := Join(testConfig(2, 128, []string{gossipAddr(seed)})); err == nil {
		bad.Shutdown() //nolint:errcheck
		t.Fatal("node with mismatched partition count must fail to join")
	}

	// The seed's view must stay clean.
	time.Sleep(300 * time.Millisecond)
	if got := len(seed.Members()); got != 1 {
		t.Fatalf("seed sees %d members, want 1", got)
	}
}

func TestDeadDetectionAndDeadSince(t *testing.T) {
	a, err := Join(testConfig(1, 256, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown() //nolint:errcheck
	b, err := Join(testConfig(2, 256, []string{gossipAddr(a)}))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, "join", func() bool { return len(a.Members()) == 2 })

	if err := b.Shutdown(); err != nil { // crash: no leave broadcast
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "dead detection", func() bool {
		return len(a.Members()) == 1
	})
	if _, ok := a.DeadSince([16]byte{2}); !ok {
		t.Fatal("DeadSince must report the crashed node")
	}
}

func TestStatusFlagPropagation(t *testing.T) {
	a, err := Join(testConfig(1, 256, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown() //nolint:errcheck
	b, err := Join(testConfig(2, 256, []string{gossipAddr(a)}))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Shutdown() //nolint:errcheck
	waitFor(t, 2*time.Second, "join", func() bool { return len(a.Members()) == 2 })

	if err := b.SetPartitionStatus(7, StatusActive); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "flag propagation", func() bool {
		for _, m := range a.Members() {
			if m.Meta.ID == ([16]byte{2}) && m.Meta.Flags.Get(7) == StatusActive {
				return true
			}
		}
		return false
	})
	// Neighbouring flags untouched.
	for _, m := range a.Members() {
		if m.Meta.ID == ([16]byte{2}) {
			if m.Meta.Flags.Get(6) != StatusNone || m.Meta.Flags.Get(8) != StatusNone {
				t.Fatal("adjacent partition flags corrupted")
			}
		}
	}
}

func TestChangedSignalCoalesces(t *testing.T) {
	a, err := Join(testConfig(1, 256, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown() //nolint:errcheck

	for i := 0; i < 10; i++ {
		a.notify()
	}
	select {
	case <-a.Changed():
	default:
		t.Fatal("expected a pending change signal")
	}
	select {
	case <-a.Changed():
		t.Fatal("signals must coalesce to one")
	default:
	}
}

func TestMetaEncodeDecodeRoundTrip(t *testing.T) {
	m := NodeMeta{ID: [16]byte{1, 2}, Partitions: 256, Generation: 42}
	m.Flags = NewPartitionFlags(256)
	m.Flags.Set(0, StatusActive)
	m.Flags.Set(255, StatusDraining)
	m.Flags.Set(100, StatusBootstrapping)

	got, err := DecodeMeta(m.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Partitions != m.Partitions || got.Generation != m.Generation {
		t.Fatalf("header mismatch: %+v", got)
	}
	for pid := uint16(0); pid < 256; pid++ {
		if got.Flags.Get(pid) != m.Flags.Get(pid) {
			t.Fatalf("flag mismatch at %d", pid)
		}
	}
	if _, err := DecodeMeta(m.Encode()[:10]); err == nil {
		t.Fatal("short meta must be rejected")
	}
}

func TestMetaSizeFitsGossipLimit(t *testing.T) {
	for _, p := range []uint16{256, 1024} {
		m := NodeMeta{Partitions: p, Flags: NewPartitionFlags(p)}
		if size := len(m.Encode()); size > memberlist.MetaMaxSize {
			t.Fatalf("P=%d: meta %d bytes exceeds memberlist cap %d", p, size, memberlist.MetaMaxSize)
		}
	}
}

func ExampleStatus() {
	fmt.Println(StatusBootstrapping, StatusActive, StatusDraining)
	// Output: bootstrapping active draining
}
