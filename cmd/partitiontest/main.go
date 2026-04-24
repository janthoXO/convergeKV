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
	r1Addr := flag.String("replica1", "localhost:50051", "Address of replica1")
	r2Addr := flag.String("replica2", "localhost:50052", "Address of replica2")
	r3Addr := flag.String("replica3", "localhost:50053", "Address of replica3")
	flag.Parse()

	log.Printf("Starting partition simulation...")

	r1Client := mustConnect(*r1Addr)
	r2Client := mustConnect(*r2Addr)
	r3Client := mustConnect(*r3Addr)
	defer r1Client.conn.Close()
	defer r2Client.conn.Close()
	defer r3Client.conn.Close()

	// 2. Isolate replica2 from convergekv network
	log.Printf("Disconnecting replica2 from network...")
	err := exec.Command("docker", "network", "disconnect", "convergekv_convergekv", "replica2").Run()
	if err != nil {
		log.Printf("Failed to disconnect replica2: %v. Ignore if already disconnected.", err) // Might fail if different compose project name
		err = exec.Command("docker", "network", "disconnect", "convergekv", "replica2").Run()
		if err != nil {
			log.Fatalf("Also failed with network 'convergekv': %v", err)
		}
	}
	time.Sleep(1 * time.Second)

	// 3. Issue Puts during partition
	log.Printf("Putting x=1 on replica1...")
	ts1, err := r1Client.kv.Put(context.Background(), &kvpb.PutRequest{Key: "x", ValueJson: `{"v": 1}`})
	if err != nil {
		log.Fatalf("Put on replica1 failed: %v", err)
	}

	log.Printf("Putting x=2 on replica2...")
	ts2, err := r2Client.kv.Put(context.Background(), &kvpb.PutRequest{Key: "x", ValueJson: `{"v": 2}`})
	if err != nil {
		log.Fatalf("Put on replica2 failed: %v", err)
	}

	// 4. Reconnect replica2
	log.Printf("Reconnecting replica2 to network...")
	err = exec.Command("docker", "network", "connect", "convergekv_convergekv", "replica2").Run()
	if err != nil {
		err = exec.Command("docker", "network", "connect", "convergekv", "replica2").Run()
		if err != nil {
			log.Fatalf("Failed to reconnect replica2: %v", err)
		}
	}

	// 5. Sleep to allow anti-entropy to heal partition
	log.Printf("Sleeping 5 seconds for anti-entropy...")
	time.Sleep(5 * time.Second)

	// 6. Get values
	log.Printf("Fetching converged value from all replicas...")
	val1 := mustGet(r1Client.kv, "x")
	val2 := mustGet(r2Client.kv, "x")
	val3 := mustGet(r3Client.kv, "x")

	fmt.Println("--- Result ---")
	fmt.Printf("Replica1 assigned TS: Phys=%d, Logic=%d\n", ts1.Timestamp.PhysicalMs, ts1.Timestamp.Logical)
	fmt.Printf("Replica2 assigned TS: Phys=%d, Logic=%d\n", ts2.Timestamp.PhysicalMs, ts2.Timestamp.Logical)
	fmt.Printf("Replica1 Value: %s\n", val1)
	fmt.Printf("Replica2 Value: %s\n", val2)
	fmt.Printf("Replica3 Value: %s\n", val3)

	if val1 != val2 || val1 != val3 {
		log.Fatalf("Convergence failed! Values are not identical across the cluster.")
	}
	fmt.Println("Convergence successful! All nodes agree.")
}

type client struct {
	conn *grpc.ClientConn
	kv   kvpb.KVServiceClient
}

func mustConnect(addr string) client {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", addr, err)
	}
	return client{conn: conn, kv: kvpb.NewKVServiceClient(conn)}
}

func mustGet(c kvpb.KVServiceClient, key string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := c.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Get on %s failed: %v", key, err)
	}
	return res.ValueJson
}
