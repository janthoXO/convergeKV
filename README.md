# ConvergeKV

ConvergeKV is a distributed, eventually-consistent key-value store written in Go. Nodes form a cluster via gossip, route writes by rendezvous hashing, and continuously reconcile state using Invertible Bloom Lookup Tables (IBLTs). Values are JSON objects merged field-by-field by a last-write-wins CRDT keyed on a Hybrid Logical Clock — so concurrent partial updates from any replica converge without coordination.

## Table of contents

- [Highlights](#highlights)
- [Quick start](#quick-start)
- [Data and consistency model](#data-and-consistency-model)
- [Component overview](#component-overview)
- [Write path (PUT / DELETE)](#write-path-put--delete)
- [Anti-entropy sync](#anti-entropy-sync)
- [Cluster membership (gossip)](#cluster-membership-gossip)
- [Bootstrap and lifecycle](#bootstrap-and-lifecycle)
- [Configuration](#configuration)
- [Developer guide](#developer-guide)

## Highlights

- **Eventually consistent** with quorum-of-one writes. Any replica in the HRW set for a key can serve reads and writes.
- **CRDT semantics at field granularity.** Concurrent partial updates from multiple writers don't clobber each other; deletes are tombstones that obey the same LWW order.
- **IBLT anti-entropy.** Pairwise reconciliation transmits O(diff) bytes for small divergences, with a transparent full-state fallback when the diff is too large to decode.
- **Hybrid Logical Clock** with bounded skew (rejects timestamps more than 10 minutes off wall time) and persistent floor seeding on restart.
- **No in-memory CRDT cache.** BadgerDB is the source of truth; the only derived in-memory state is the IBLT used for sync.

## Quick start

Three-node local cluster via Docker Compose:

```bash
make docker-up      # builds the image and starts replica1/2/3
# replica1 → localhost:50051, replica2 → localhost:50052, replica3 → localhost:50053
make docker-down    # tear down + remove volumes
```

The gRPC surface is defined in [proto/kv/kv.proto](proto/kv/kv.proto) (client API: `Put`, `Get`, `Delete`, `Status`). Any replica accepts any key — internally the request is forwarded to an HRW replica if the receiving node isn't itself one.

For local builds, tests, and the full developer workflow see [README_DEV.md](README_DEV.md).

## Data and consistency model

A value is a JSON object stored at a string key. Internally, each `(key, field)` pair is an independent **Add-Wins Last-Write-Wins Map (AWLWWMap)** entry: a `FieldEntry` carrying the value, an HLC timestamp, the originating replica ID, and a `Deleted` tombstone flag. Two replicas that disagree on a `(key, field)` resolve the conflict by picking the higher HLC (with replica-ID as the deterministic tiebreaker) — so partial updates and deletes commute and converge.

- **Virtual partitions:** keys are mapped to partitions by `keyspace.Of(key, P) = xxhash64(key) % P`. All `(key, field)` pairs for one key always land in the same partition. `NUM_PARTITIONS` (`P`) must be identical on every node — disagreement silently splits the cluster.
- **Storage key encoding:** `partitionID(4B big-endian) + key + "\x00" + field`. Each partition is a contiguous Badger range. Client-supplied keys containing `\x00` are rejected at the API boundary.
- **Replication factor `RF`:** each partition is placed on the top `RF` replicas by HRW score over `(partitionID, replicaID)`. No central placement table. With `RF < node count`, each node owns and stores only a fraction of the keyspace.
- **Quorum:** writes ack after one local persist; the other replicas receive the entry via fire-and-forget push and, on failure, reconcile in the next anti-entropy round.

## Component overview

| Package | Responsibility |
|---|---|
| `cmd/server` | Process entry point; wires every subsystem together. |
| `internal/api` | gRPC handlers for `KVService`, `ForwardService`, and `DebugService`. Defines consumer interfaces; no concrete subsystem imports. |
| `internal/coordinator` | Routes PUT/GET/DELETE by partition; forwards or serves locally; triggers write-path push. |
| `internal/replica` | Central state holder. Owns the HLC, the per-key lock table, storage, and IBLT references. `RecoverHLCFloor` seeds the HLC at startup. |
| `internal/storage` | BadgerDB wrapper. Partition-prefixed key encoding (`keyspace`), `IteratePartition`, atomic `SaveBatch`. No HLC dependency. |
| `internal/domain/crdt` | `FieldEntry`, AWLWW merge rules, `WinsOver` predicate. |
| `internal/domain/hlc` | Hybrid Logical Clock with `Send`/`Receive`/`Seed` and bounded skew detection. |
| `internal/domain/keyspace` | Maps keys to partition IDs (`xxhash64 % P`), encodes/decodes storage keys, validates null bytes. |
| `internal/cluster/placement` | Generic stateless rendezvous hashing `Owners[M Member](pid, members, rf)`. No gossip dependency. |
| `internal/cluster/ownership` | Tracks which partitions the local node owns and who the co-owners are. Drives IBLT lifecycle (`EnsurePartition`/`DropPartition`) on membership changes via the gossip `Subscribe` channel. |
| `internal/gossip` | HashiCorp memberlist wrapper. Fan-out `Subscribe()` channels replace a single `OnChange` callback — each subscriber gets its own buffered channel. |
| `internal/connpool` | Shared, evict-on-leave gRPC connection pool used by forwarder and replication. |
| `internal/iblt` | General-purpose IBLT (3-hash, length-prefixed, XOR-folded cells) with `Decode` and `Subtract`. |
| `internal/replication/antientropy` | IBLT-based initiator loop: per-partition three-step reconciliation with full-state fallback. |
| `internal/replication/grpcsrv` | `SyncService` gRPC server (inbound `GetIBLT`, `PushEntries`, `PullEntries`). |
| `internal/replication/pushfanout` | Write-path push fanout (`PushToPeers`): fans entries to co-replicas after a local write. |
| `internal/replication/protoconv` | Single home for proto↔domain entry conversion (`EntryToProto`/`ProtoToEntry`). |

## Write path (PUT / DELETE)

A client request is validated at the API boundary, routed by HRW, served locally if this node owns the key, and asynchronously fanned out to the other replicas. If the local node is not in the HRW replica set, the coordinator forwards to the highest-scoring replica (with fall-through to the next on failure). The push is fire-and-forget; any replica that misses an update will reconcile on the next anti-entropy round — that part is covered by the [sync diagram](#anti-entropy-sync).

![PUT workflow](docs/put.svg)

Key invariants:

- The per-key mutex is acquired *before* `HLC.Send`, so timestamps on the same key are issued in the order they will be persisted.
- `SaveBatch` wraps every batch in a single Badger `db.Update` transaction, so a multi-field PUT is all-or-nothing.
- `Put`/`Delete` return the exact entries written; the coordinator pushes those bytes directly without a second Badger read.

## Anti-entropy sync

Every `SYNC_MS` milliseconds, each node iterates its owned partitions (derived from live gossip membership via HRW) and for each `(partition, co-owner)` pair runs an initiator-driven three-step reconciliation. The protocol is bounded by a 30 s per-pair timeout.

1. **`GetIBLT`** (unary) — fetch the peer's IBLT snapshot.
2. **Diff locally** — subtract the two IBLTs and decode the result into `onlyLocal` / `onlyRemote` item sets.
3. **`PushEntries` + `PullEntries`** (concurrent streams) — push what the peer is missing while pulling what we are missing.

If `Decode` fails (typically when the symmetric difference exceeds ~250 items for the default 512-cell IBLT, or any cell remains impure) the protocol falls back to a **full-partition exchange**: stream the entire partition via `IteratePartition` while concurrently pulling the peer's full partition via `PullEntries` with an empty identifier list. Both sides paginate in 1000-record pages so neither buffers the dataset.

![Sync workflow](docs/sync.svg)

Each IBLT item is a fixed binary encoding of `(key, field, HLC, replicaID, deleted)`, so an entry that's been superseded since the IBLT snapshot no longer matches and the next round naturally re-reconciles the new version.

## Cluster membership (gossip)

Cluster membership is handled by HashiCorp memberlist over UDP. Nodes join via the `SEEDS` env var with exponential-backoff retries; `gossip.Start` blocks until the first successful join, so a node never starts serving without a cluster view.

Memberlist's event callbacks fire while it holds its own lock, so the gossip layer signals through a buffered channel and a dedicated `changeWorker` rebuilds the membership view *outside* that lock. This is what avoids AB-BA deadlocks with any consumer that calls back into `gossip.Members()`. Each `g.Subscribe()` call returns an independent buffered channel (cap 1, coalescing); consumers process membership changes serially in their own goroutines. `main.go` takes two subscriptions: one drives `pool.EvictAbsent` and one drives `ownership.Run`, which also kicks the IBLT lifecycle when the owned partition set changes.

![Gossip workflow](docs/gossip.svg)

The shared `connpool.Pool` is the single source of gRPC connections used by both the coordinator (client forwarding) and the syncer (anti-entropy + write-path push), so evicting a departed peer removes it from every code path at once.

## Bootstrap and lifecycle

The server entry point ([cmd/server/main.go](cmd/server/main.go)) wires the subsystems in dependency order, blocks on a successful gossip join, then starts the gRPC server and the anti-entropy loop. The HLC is seeded via `replica.RecoverHLCFloor(store)` — a scan of all persisted entries — so a backwards NTP correction or a fresh-process restart can never issue a timestamp that loses to data already on disk.

![Bootstrap workflow](docs/bootstrap.svg)

Shutdown is signal-driven (SIGINT/SIGTERM) and ordered: stop new RPCs, drain in-flight push goroutines, close the connection pool, leave gossip, then flush Badger.

## Configuration

All configuration is via environment variables, parsed at startup:

| Variable | Default | Description |
|---|---|---|
| `REPLICA_ID` | required | Unique node identifier. |
| `GRPC_PORT` | `50051` | gRPC listen port. |
| `GOSSIP_PORT` | `7946` | memberlist UDP port. |
| `GOSSIP_BIND` | `0.0.0.0` | memberlist bind address. |
| `SEEDS` | `""` | Comma-separated `host:port` peers to join. |
| `DATA_DIR` | `/data` | BadgerDB data directory. |
| `RF` | `3` | **Cluster-wide constant.** Replication factor — each partition placed on `RF` nodes via HRW. Must be identical on every node. |
| `NUM_PARTITIONS` | `512` | **Cluster-wide constant.** Virtual partition count. Must be identical on every node; changing it requires a fresh `DATA_DIR`. |
| `SYNC_MS` | `2000` | Anti-entropy sync interval in milliseconds. |
| `IBLT_CELLS` | `512` | **Cluster-wide constant.** IBLT cell count per partition. Must be identical on every node — a mismatch is reported via a gossip config-fingerprint warning at join time. |

## Developer guide

See [README_DEV.md](README_DEV.md) for build, test, protobuf regeneration, the package-by-package source map, and the conventions to follow when changing the wire protocol or storage encoding.
