// Command partitiontest is a standalone integration demo for ConvergeKV.
// It simulates a network partition by disconnecting replica2 from the cluster,
// writes conflicting values to replica1 and replica2, then reconnects and
// verifies that all three replicas converge to the same value via anti-entropy.
//
// Usage:
//
//	go run ./cmd/partitiontest --replica1=localhost:50051 \
//	                            --replica2=localhost:50052 \
//	                            --replica3=localhost:50053
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
)

func main() {
	r1Addr := flag.String("replica1", "localhost:50051", "address of replica1")
	r2Addr := flag.String("replica2", "localhost:50052", "address of replica2")
	r3Addr := flag.String("replica3", "localhost:50053", "address of replica3")
	flag.Parse()

	ctx := context.Background()

	// 1. Dial all three replicas.
	c1 := dialKV(ctx, *r1Addr)
	c2 := dialKV(ctx, *r2Addr)
	c3 := dialKV(ctx, *r3Addr)

	// 2. Simulate partition: disconnect replica2 from the convergekv network.
	fmt.Println("==> Simulating partition: disconnecting replica2 ...")
	runDocker("network", "disconnect", "convergekv", "replica2")
	time.Sleep(500 * time.Millisecond)

	// 3. Write conflicting values while partitioned.
	fmt.Println("==> Writing {\"v\":1} to replica1 ...")
	r1Put, err := c1.Put(ctx, &kvpb.PutRequest{Key: "x", ValueJson: `{"v":1}`})
	if err != nil {
		log.Fatalf("Put replica1: %v", err)
	}
	fmt.Printf("    replica1 timestamp: phys=%d logical=%d\n",
		r1Put.Timestamp.PhysicalMs, r1Put.Timestamp.Logical)

	fmt.Println("==> Writing {\"v\":2} to replica2 ...")
	r2Put, err := c2.Put(ctx, &kvpb.PutRequest{Key: "x", ValueJson: `{"v":2}`})
	if err != nil {
		log.Fatalf("Put replica2: %v", err)
	}
	fmt.Printf("    replica2 timestamp: phys=%d logical=%d\n",
		r2Put.Timestamp.PhysicalMs, r2Put.Timestamp.Logical)

	// 4. Heal the partition.
	fmt.Println("==> Healing partition: reconnecting replica2 ...")
	runDocker("network", "connect", "convergekv", "replica2")

	// 5. Wait for anti-entropy to propagate.
	fmt.Println("==> Waiting 5s for anti-entropy convergence ...")
	time.Sleep(5 * time.Second)

	// 6. Read from all three replicas.
	g1 := getKV(ctx, c1, "x")
	g2 := getKV(ctx, c2, "x")
	g3 := getKV(ctx, c3, "x")

	fmt.Printf("\n==> Results after convergence:\n")
	fmt.Printf("    replica1: %s\n", g1)
	fmt.Printf("    replica2: %s\n", g2)
	fmt.Printf("    replica3: %s\n", g3)

	// 7. Explain the winner.
	fmt.Printf("\n==> Analysis:\n")
	fmt.Printf("    replica1 write ts: phys=%d logical=%d\n", r1Put.Timestamp.PhysicalMs, r1Put.Timestamp.Logical)
	fmt.Printf("    replica2 write ts: phys=%d logical=%d\n", r2Put.Timestamp.PhysicalMs, r2Put.Timestamp.Logical)
	if r1Put.Timestamp.PhysicalMs > r2Put.Timestamp.PhysicalMs ||
		(r1Put.Timestamp.PhysicalMs == r2Put.Timestamp.PhysicalMs &&
			r1Put.Timestamp.Logical > r2Put.Timestamp.Logical) {
		fmt.Println("    Winner: replica1 (higher HLC timestamp) → v=1")
	} else if r2Put.Timestamp.PhysicalMs > r1Put.Timestamp.PhysicalMs ||
		(r2Put.Timestamp.PhysicalMs == r1Put.Timestamp.PhysicalMs &&
			r2Put.Timestamp.Logical > r1Put.Timestamp.Logical) {
		fmt.Println("    Winner: replica2 (higher HLC timestamp) → v=2")
	} else {
		fmt.Println("    Tie on HLC — winner decided by replica_id lexicographic order (replica2 > replica1) → v=2")
	}

	// 8. Assert convergence.
	if g1 != g2 || g2 != g3 {
		log.Fatalf("CONVERGENCE FAILURE: r1=%s r2=%s r3=%s", g1, g2, g3)
	}
	fmt.Println("\n✅ All three replicas converged to the same value.")
}

// dialKV dials a KVService gRPC endpoint.
func dialKV(_ context.Context, addr string) kvpb.KVServiceClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return kvpb.NewKVServiceClient(conn)
}

// getKV returns the JSON value for a key or "<not found>" if absent.
func getKV(ctx context.Context, c kvpb.KVServiceClient, key string) string {
	resp, err := c.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		return fmt.Sprintf("<error: %v>", err)
	}
	if !resp.Found {
		return "<not found>"
	}
	return resp.ValueJson
}

// runDocker executes a docker CLI command, logging any error but not fataling.
func runDocker(args ...string) {
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[docker %v] error: %v\n%s", args, err, out)
	}
}
