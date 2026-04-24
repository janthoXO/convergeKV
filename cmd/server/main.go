package main

import (
    "context"
    "fmt"
    "log"
    "net"
    "os"
    "strings"
    "time"

    "google.golang.org/grpc"

    kvpb  "github.com/janthoXO/convergeKV/gen/kv"
    repb  "github.com/janthoXO/convergeKV/gen/replication"
    "github.com/janthoXO/convergeKV/internal/api"
    "github.com/janthoXO/convergeKV/internal/node"
    "github.com/janthoXO/convergeKV/internal/replication"
    "github.com/janthoXO/convergeKV/internal/storage"
)

func main() {
    replicaID  := mustEnv("REPLICA_ID")
    peersRaw   := os.Getenv("PEERS")           // comma-separated host:port, may be empty
    grpcPort   := envOr("GRPC_PORT", "50051")
    dataDir    := envOr("DATA_DIR", "/data")
    syncMs     := 2000 // anti-entropy interval in ms

    peers := []string{}
    if peersRaw != "" {
        for _, p := range strings.Split(peersRaw, ",") {
            if t := strings.TrimSpace(p); t != "" {
                peers = append(peers, t)
            }
        }
    }

    // Storage
    store, err := storage.Open(dataDir)
    if err != nil {
        log.Fatalf("open storage: %v", err)
    }
    defer store.Close()

    // Node
    n, err := node.New(replicaID, store)
    if err != nil {
        log.Fatalf("create node: %v", err)
    }

    // Causal context
    causal := replication.NewCausalContext()

    // gRPC server
    srv := grpc.NewServer()
    kvpb.RegisterKVServiceServer(srv, api.NewHandler(n, peers, causal))
    repb.RegisterReplicationServiceServer(srv, replication.NewHandler(n, causal))

    lis, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
    if err != nil {
        log.Fatalf("listen: %v", err)
    }
    log.Printf("[%s] listening on :%s  peers=%v", replicaID, grpcPort, peers)

    // Anti-entropy
    ae := replication.NewAntiEntropy(n, peers, causal, time.Duration(syncMs)*time.Millisecond)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go ae.Run(ctx)

    // Serve (blocking)
    if err := srv.Serve(lis); err != nil {
        log.Fatalf("serve: %v", err)
    }
}

func mustEnv(key string) string {
    v := os.Getenv(key)
    if v == "" {
        log.Fatalf("required env var %s is not set", key)
    }
    return v
}

func envOr(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}
