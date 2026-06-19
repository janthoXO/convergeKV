package clustertest

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// TestBenchmarkReport measures p50/p99 write and read latency for 1KB
// documents plus replication convergence latency on a 5-node in-process
// cluster. Gated behind CONVERGEKV_BENCH=1; results go to docs/BENCHMARKS.md
// by hand.
func TestBenchmarkReport(t *testing.T) {
	if os.Getenv("CONVERGEKV_BENCH") == "" {
		t.Skip("set CONVERGEKV_BENCH=1 to run the benchmark report")
	}
	h := Start(t, 5)
	ctx := context.Background()
	client := h.Client(0)

	payload := fmt.Sprintf(`{"data": %q}`, strings.Repeat("x", 1000)) // ≈1KB document
	doc := []byte(payload)

	// Warm-up.
	for i := 0; i < 200; i++ {
		if _, err := client.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("warm-%d", i), Value: doc}); err != nil {
			t.Fatal(err)
		}
	}

	const samples = 2000
	putLat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if _, err := client.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("bench-%d", i%256), Value: doc}); err != nil {
			t.Fatal(err)
		}
		putLat = append(putLat, time.Since(start))
	}
	getLat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if _, err := client.Get(ctx, &pb.GetRequest{Key: fmt.Sprintf("bench-%d", i%256)}); err != nil {
			t.Fatal(err)
		}
		getLat = append(getLat, time.Since(start))
	}

	// Convergence latency: write, then poll until all owners are
	// byte-identical for the new value.
	convLat := make([]time.Duration, 0, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("conv-%d", i)
		start := time.Now()
		if _, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: doc}); err != nil {
			t.Fatal(err)
		}
		h.WaitOwnersConverged(key, 5*time.Second)
		convLat = append(convLat, time.Since(start))
	}

	report := func(name string, lat []time.Duration) {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		t.Logf("%-22s p50=%-10v p99=%-10v max=%v",
			name, lat[len(lat)/2], lat[len(lat)*99/100], lat[len(lat)-1])
	}
	t.Logf("5-node in-process cluster, %d partitions, 1KB documents", Partitions)
	report("put (applier persist)", putLat)
	report("get (single owner)", getLat)
	report("convergence (RF=3)", convLat)
}
