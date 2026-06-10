# ConvergeKV — developer guide

This document is the working companion to [README.md](README.md). It collects the day-to-day commands, the protobuf workflow, and the deeper source-level notes about how each subsystem is wired — enough to land a non-trivial change without grepping cold.

## Table of contents

- [Prerequisites](#prerequisites)
- [Make targets](#make-targets)
- [Local development workflows](#local-development-workflows)
  - [Protobuf regeneration](#protobuf-regeneration)
  - [Running tests](#running-tests)
  - [Running a local cluster](#running-a-local-cluster)
  - [Single-process / single-test runs](#single-process--single-test-runs)
- [Repository layout](#repository-layout)
- [How the components fit together](#how-the-components-fit-together)
  - [Process bootstrap (`cmd/server`)](#process-bootstrap-cmdserver)
  - [API surface (`internal/api`)](#api-surface-internalapi)
  - [Routing (`internal/coordinator`, `internal/cluster/placement`)](#routing-internalcoordinator-internalclusterplacement)
  - [Replica and storage (`internal/replica`, `internal/storage`)](#replica-and-storage-internalreplica-internalstorage)
  - [CRDT and HLC (`internal/domain/crdt`, `internal/domain/hlc`)](#crdt-and-hlc-internaldomaincrdt-internaldomainhlc)
  - [Anti-entropy (`internal/iblt`, `internal/replication/...`)](#anti-entropy-internaliblt-internalreplication)
  - [Gossip and connection pool (`internal/gossip`, `internal/connpool`)](#gossip-and-connection-pool-internalgossip-internalconnpool)
- [Diagrams](#diagrams)
- [Wire-protocol conventions](#wire-protocol-conventions)
- [Adding or changing an RPC](#adding-or-changing-an-rpc)
- [Debugging tips](#debugging-tips)

## Prerequisites

- Go (matching the `go.mod` directive; the Docker build pins `golang:1.26-alpine`).
- `protoc` and the Go gRPC plugins on `$PATH`, only needed when changing `.proto` files:
  - `protoc-gen-go`
  - `protoc-gen-go-grpc`
- Docker + Docker Compose for the multi-node cluster.
- Mermaid diagrams in `docs/*.mermaid` are compiled to SVG automatically by [.github/workflows/mermaid.yml](.github/workflows/mermaid.yml) on pushes to `main`. Don't commit the SVGs by hand — edit the `.mermaid` source.

## Make targets

```bash
make proto        # regenerate gRPC/protobuf Go code from proto/ definitions
make build        # compile server + partitiontest into dist/
make test         # go test ./... -race -count=1
make lint         # go vet ./...
make format       # gofmt -s -w .
make docker-up    # build image, start replica1/2/3
make docker-down  # tear down cluster and volumes
```

## Local development workflows

### Protobuf regeneration

Wire definitions live under [proto/](proto/) (`kv/`, `replication/`, `forward/`) and generated Go ends up at [gen/](gen/). After editing a `.proto` file:

```bash
make proto
```

The Makefile invokes `protoc` with `paths=source_relative` so the generated package layout mirrors the source tree. Commit both the `.proto` change and the regenerated `.pb.go` files in the same commit.

### Running tests

```bash
make test                                               # full suite, race detector, no cache
go test ./internal/replication/antientropy -race -v    # single package
go test -run TestIBLTDecode ./internal/iblt             # single test
```

Many tests spin up real BadgerDB instances in a `t.TempDir()`, and the anti-entropy tests bring up small in-process clusters — they run quickly but expect a few hundred milliseconds of headroom.

### Running a local cluster

`docker-compose.yml` defines three nodes that gossip to each other on `:7946` and expose gRPC on host ports `50051` / `50052` / `50053`. Their seed lists are set up so any one of them can come up first and the rest will join when they appear.

```bash
make docker-up
# tail one of the replicas
docker logs -f replica1
# tear it down (volumes too)
make docker-down
```

Talk to a node with any gRPC client (e.g. `grpcurl`):

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"key":"u1","valueJson":"{\"name\":\"alice\"}"}' \
        localhost:50051 kv.KVService/Put
grpcurl -plaintext -d '{"key":"u1"}' localhost:50051 kv.KVService/Get
```

`gRPC reflection` is registered unconditionally in [cmd/server/main.go](cmd/server/main.go), which is what makes the `grpcurl list` discovery work out of the box.

### Single-process / single-test runs

The `cmd/server` binary reads its config from env vars (a `.env` file in the working directory is also picked up via `godotenv`). For a one-off local instance:

```bash
REPLICA_ID=local1 DATA_DIR=/tmp/ckv-local1 GRPC_PORT=50051 \
GOSSIP_PORT=7946 SEEDS="" RF=1 \
go run ./cmd/server
```

`RF=1` plus an empty `SEEDS` gives a single-replica setup that's handy for poking at the API without standing up a cluster.

There is also a [cmd/partitiontest](cmd/partitiontest/main.go) binary used by hand for inducing partitions against a running cluster; build it via `make build` and run from `dist/`.

## Repository layout

```
cmd/
  server/                    main.go — process entry; wires everything together
  partitiontest/             ad-hoc tool for partition experiments
proto/                       gRPC service definitions (source of truth)
  kv/kv.proto                client-facing KVService (Put/Get/Delete/Status)
  forward/forward.proto      internal ForwardService (peer-to-peer client forwarding)
  replication/replication.proto  SyncService (GetIBLT, PushEntries, PullEntries)
  debug/debug.proto          DebugService (ScanAll — streams all entries on this node)
gen/                         generated Go (DO NOT edit; run `make proto`)
internal/
  api/                       KVService + ForwardService + DebugService handlers; consumer interfaces
  coordinator/               partition routing, forwarding, async push fan-out
  replica/                   per-key locks, HLC, Put/Get/Delete/ApplyDelta; RecoverHLCFloor
  storage/                   BadgerDB wrapper, SaveBatch, paginated IterateAll (no HLC dependency)
  domain/
    crdt/                    FieldEntry, AWLWW merge, WinsOver predicate
    hlc/                     Hybrid Logical Clock (Send/Receive/Seed)
    keyspace/                partition mapping (xxhash), storage key encoding, null-byte validation
  cluster/
    placement/               generic stateless rendezvous hashing Owners[M Member](pid, members, rf)
    ownership/               owned-partition cache; drives IBLT lifecycle on membership changes
  replication/
    antientropy/             IBLT-based initiator loop (three-step protocol + full-state fallback)
    grpcsrv/                 SyncService gRPC server (GetIBLT, PushEntries, PullEntries)
    pushfanout/              write-path push fanout (PushToPeers)
    protoconv/               proto↔domain entry conversion (EntryToProto / ProtoToEntry)
  iblt/                      general-purpose IBLT (Insert/Delete/Subtract/Decode) + IBLTState
  gossip/                    memberlist wrapper + Subscribe fan-out channels
  connpool/                  shared gRPC connection pool (evict-on-leave)
  utils/                     small slice helpers
docs/                        mermaid sources + auto-generated SVGs (see workflow)
docker-compose.yml           3-node local cluster
Dockerfile                   multi-stage build → /convergekv
Makefile                     proto / build / test / format / docker targets
```

## How the components fit together

### Process bootstrap (`cmd/server`)

[main.go](cmd/server/main.go) is the only place subsystems are wired. The ordering matters and is intentional — see the [bootstrap diagram](#diagrams):

1. Parse env config.
2. Open BadgerDB (`storage.Open`).
3. Call `replica.RecoverHLCFloor(store)` — scans all persisted entries and returns the highest HLC timestamp — and pass it as `replica.WithHLCFloor(...)` so the HLC can never go backwards across a restart.
4. Start gossip — `joinWithRetry` blocks here until at least one seed is reachable, so by the time we register handlers we already have a cluster view.
5. Construct `IBLTState` (initially empty); create an `ibltLifecycle` adapter that wires it to the `ownership.IndexLifecycle` interface.
6. Construct `ownership` and call `own.Update(g.Members())` synchronously so IBLTs are seeded for the initial partition set.
7. Start two gossip subscribers: one for `pool.EvictAbsent` (departed peers), one for `own.Run` (ownership + IBLT lifecycle on membership changes).
8. Construct `replica`, `pushfanout.Fanout`, `antientropy.Syncer`, forwarder, and coordinator.
9. Register the four gRPC services (`KVService`, `ForwardService`, `SyncService`, `DebugService`) and start listening.
10. Kick off `ae.Run()` in a goroutine.
11. Serve until SIGINT/SIGTERM; then shutdown in reverse order: GracefulStop → ae.Close → fanout.Close → pool.Close → gossip.Leave → store.Close.

### API surface (`internal/api`)

[handler.go](internal/api/handler.go) implements `KVService` (`Put`/`Get`/`Delete`/`Status`). The handler is intentionally thin — it validates the key (`validateKey` rejects `\x00` because the storage layer uses it as the `(key, field)` delimiter) and hands off to the coordinator via the `KV` interface. `Status` is served through a `StatusProvider` interface; a `statusFacade` in `main.go` wires it to the replica + gossip instances. The `api` package has no imports of `coordinator`, `replica`, or `gossip` — it only depends on its own consumer-defined interfaces.

[forward_handler.go](internal/api/forward_handler.go) implements `ForwardService`, the internal RPC the coordinator uses when this node isn't in the HRW set for a key. It also uses the `KV` interface.

[debug_handler.go](internal/api/debug_handler.go) implements `DebugService` (`ScanAll`), which streams every persisted `(key, field)` entry on this node. Useful for inspection and testing without needing to know which partitions a node owns.

### Routing (`internal/coordinator`, `internal/cluster/placement`)

[coordinator.go](internal/coordinator/coordinator.go) decides, per request, whether this node is one of the `RF` HRW replicas:

- If yes, it calls into the `LocalReplica` interface (`Put`/`Get`/`Delete`), returns the response synchronously, and then fans the same entries out to the other replicas via the `PushSyncer` interface (satisfied by `pushfanout.Fanout`, 2 s timeout).
- If no, `forwardWithRetry` tries each replica in HRW-score order and aborts when the request context is cancelled.

The coordinator depends only on the `LocalReplica` and `PushSyncer` consumer interfaces — it has no import of the concrete `replica` or `pushfanout` packages.

[placement.go](internal/cluster/placement/placement.go) is stateless: `Owners[M Member](pid, members, rf)` is a pure generic function over any type with `ID() string` and `Addr() string`. It has no dependency on the `gossip` package — `gossip.MemberInfo` satisfies the `Member` constraint by implementing those methods.

### Replica and storage (`internal/replica`, `internal/storage`)

[replica.go](internal/replica/replica.go) owns three things:

- The `HLC` (its own mutex; safe to call without any other lock held).
- The per-key reference-counted `keyLocks` table, used only on the write path. Reads (`GetKey`/`GetField`) hit Badger directly and rely on its MVCC.
- References to the `Store` and `Index` (IBLT state), injected at construction via concrete `*storage.Store` and `*iblt.IBLTState`.

`Put` / `Delete` ([write.go](internal/replica/write.go)) follow the same pattern:

```
acquireKey(key)               // per-key mutex; ref-counted
hlc.Send()                    // inside the lock — order matches persistence
store.GetKey(key)             // read existing fields to compute IBLT removals
store.SaveBatch(updates)      // single atomic db.Update
ibltState.Remove(old) / Insert(new)
return ts, updates
```

`ApplyDelta` ([merge.go](internal/replica/merge.go)) is the inverse — used by the push handler and anti-entropy. It uses `hlc.Receive` to advance the local clock from a remote timestamp and `crdt.WinsOver` to skip stale entries.

`RecoverHLCFloor` ([recover.go](internal/replica/recover.go)) scans all persisted entries via the `Store` interface to find the highest HLC timestamp; called once at startup to seed the clock.

[storage/badger.go](internal/storage/badger.go) is the only file that touches Badger:

- **Key encoding** — delegates to `keyspace.EncodeKey`/`DecodeKey`; the 4-byte partition prefix makes each partition a contiguous Badger range.
- **`SaveBatch`** wraps the whole batch in a single `db.Update`; either the whole multi-field write commits or nothing does.
- **`IterateAll`** pages the scan into 1000-record blocks, each in its own short-lived read transaction, so a slow network consumer (full-state pull) doesn't pin Badger's MVCC GC.
- Storage imports only `domain/crdt` and `domain/keyspace` — no HLC dependency.

### CRDT and HLC (`internal/domain/crdt`, `internal/domain/hlc`)

`crdt.FieldEntry` is the unit of state: `{Value, Timestamp, ReplicaID, Deleted}`. `crdt.WinsOver(a, b)` compares HLC first and falls back to `ReplicaID` for ties; `merge.go` implements the AWLWW map merge used by `ApplyDelta` and the tests. `crdt.NewFieldEntry(value, physMs, logical, replicaID, deleted)` constructs a `FieldEntry` from flat components, letting storage avoid a direct `hlc` import.

`hlc.HLC` is the standard CLOCK-SI Hybrid Logical Clock with three entry points:

- **`Send()`** before originating a local event (write). Rejects with `ErrClockDrift` if the HLC has drifted more than `MAX_CLOCK_DRIFT_MS` (10 min) ahead of wall time.
- **`Receive(remote)`** when applying a remote event. Rejects with the same error if the *remote* timestamp is that far ahead.
- **`Seed(floor)`** at startup, called by `WithHLCFloor`, to raise the clock above whatever's already on disk.

Both packages are pure domain packages with no internal imports — they sit at the bottom of the dependency graph.

### Anti-entropy (`internal/iblt`, `internal/replication/...`)

[iblt.go](internal/iblt/iblt.go) is a general-purpose IBLT. Items are opaque `[]byte`; the IBLT stores three values per cell (`Count`, `KeySum`, `HashSum`) and uses 3 deduplicated hash positions per item (`h1 + i·h2 mod n`, with `h2 |= 1` so the sequence visits `n` distinct slots before repeating). `Decode` peels pure cells iteratively and returns `(onlyInA, onlyInB, ok)`; `ok == false` means the symmetric difference was too large.

[iblt/iblt_state.go](internal/iblt/iblt_state.go) is the IBLT *as seen by the rest of the system*. Each item is the canonical fixed-width serialisation of `(key, field, HLC, replicaID, deleted)`. The replica's write path calls `InsertEntry`/`RemoveEntry` on this state object on every `Put`/`Delete`; `BuildFromStore` rebuilds it from Badger at startup. `EnsurePartition` seeds an IBLT for a newly-owned partition by scanning storage — this is called by `ownership.Run` whenever the local node gains a partition, fixing the latent bug where IBLTs were only built once at startup.

[replication/antientropy/antientropy.go](internal/replication/antientropy/antientropy.go) is the initiator of the three-step protocol described in the [sync diagram](#diagrams). Read it together with [replication/grpcsrv/handler.go](internal/replication/grpcsrv/handler.go) (the server side) — both halves are short and the symmetry is the easiest way to reason about the protocol. `SyncPartitionWithPeer` also implements the full-state fallback by concurrently `PushEntries`-streaming the entire local partition and `PullEntries`-receiving the peer's entire partition; both sides iterate Badger lazily via the paginated `IteratePartition`.

[replication/pushfanout/fanout.go](internal/replication/pushfanout/fanout.go) (`PushToPeers`) is invoked by the coordinator after a local write. It reuses the same `SyncService.PushEntries` wire format — one wire shape for "apply these entries to that peer", shared between the write path and anti-entropy.

[replication/protoconv/conv.go](internal/replication/protoconv/conv.go) is the single home for proto↔domain conversions (`EntryToProto`/`ProtoToEntry`), used by both the push fanout and the anti-entropy initiator.

### Gossip and connection pool (`internal/gossip`, `internal/connpool`)

[gossip.go](internal/gossip/gossip.go) wraps HashiCorp memberlist. The non-obvious bit is the `changeCh` + `changeWorker` pattern: memberlist invokes event callbacks while holding its own lock, so the eventDelegate just signals a buffered channel and a dedicated goroutine rebuilds `g.members` *outside* the memberlist lock. This is what makes it safe for any consumer (`placement`, `ownership`, the `Status` handler) to call back into `gossip.Members()` from arbitrary goroutines.

`Subscribe()` returns a buffered channel (cap 1, coalescing) that receives the full membership list on every change. Multiple callers each get their own channel — `main.go` takes two: one for `pool.EvictAbsent` and one for `ownership.Run`. Non-blocking sends ensure a slow consumer doesn't stall others.

`joinWithRetry` blocks `Start` until the first successful join with exponential backoff capped at 30 s — so a node never starts serving requests without a cluster view.

[connpool/pool.go](internal/connpool/pool.go) is a tiny `map[string]*grpc.ClientConn` keyed by `host:port`. `EvictAbsent(keep)` snapshots the to-close connections under the mutex, releases it, *then* calls `Close()` — so a slow `Close()` cannot stall concurrent `Get` callers.

## Diagrams

The mermaid sources live in [docs/](docs/) and are compiled to SVG automatically by [.github/workflows/mermaid.yml](.github/workflows/mermaid.yml). To preview locally:

```bash
npx -p @mermaid-js/mermaid-cli mmdc -i docs/put.mermaid -o /tmp/put.svg
```

Source files:

- [docs/put.mermaid](docs/put.mermaid) — write path (PUT/DELETE)
- [docs/sync.mermaid](docs/sync.mermaid) — anti-entropy
- [docs/gossip.mermaid](docs/gossip.mermaid) — membership + change worker
- [docs/bootstrap.mermaid](docs/bootstrap.mermaid) — process boot/shutdown

## Wire-protocol conventions

- The storage key encoding uses `\x00` as the `(key, field)` delimiter. **Client-supplied** keys are rejected by `api.validateKey` (delegates to `keyspace.RejectNullBytes`). Peer-supplied keys arriving on `SyncService` (`PushEntries`, `PullEntries`) are not currently validated — see `N1` in [docs/REVIEW.md](docs/REVIEW.md).
- Every `DeltaEntry` carries the full `(key, field, value, HLC, replicaID, deleted)` tuple. Tombstones are normal entries with `Deleted=true` and are persisted, IBLT-tracked, and replicated identically to live values. `protoconv.EntryToProto`/`ProtoToEntry` are the single home for this conversion.
- IBLT items use a fixed-width binary encoding ([iblt/item.go](internal/iblt/item.go) `serialiseItem`/`DeserialiseItem`); changing it requires bumping every node simultaneously (no version negotiation exists yet).

## Adding or changing an RPC

1. Edit the relevant `.proto` under [proto/](proto/).
2. Run `make proto` to regenerate `gen/`.
3. Update the server-side handler:
   - Client API → `internal/api/handler.go`
   - Internal forwarding → `internal/api/forward_handler.go`
   - Anti-entropy / replication → `internal/replication/grpcsrv/handler.go`
   - Debug → `internal/api/debug_handler.go`
4. Update the client-side caller (the coordinator's forwarder for `ForwardService`; the anti-entropy initiator in `internal/replication/antientropy/` for `SyncService`).
5. Add a focused test in the package that owns the handler. The anti-entropy tests (`internal/replication/antientropy/`) are a good template for end-to-end coverage; the storage tests are a good template for round-trip encoding coverage.

## Debugging tips

- All log lines are prefixed with the originating subsystem in square brackets (`[gossip]`, `[antientropy]`, `[grpcsrv]`, `[pushfanout]`, `[coordinator]`, `[hlc]`, etc.). Filter with `grep` rather than reading the full stream.
- `Status` RPC returns the live gossip view as seen by the node you query — useful to confirm membership before suspecting replication.
- `DebugService.ScanAll` streams every `(key, field)` entry persisted on a given node — useful for checking what a node actually holds without needing to know partition assignments.
- An `ErrClockDrift` log line from `[antientropy]` means a peer is more than 10 minutes off wall time. Fix the clock; do not raise `MAX_CLOCK_DRIFT_MS` ([domain/hlc/hlc.go](internal/domain/hlc/hlc.go)).
- If anti-entropy looks stuck, check whether `Decode` is failing (look for `IBLT diff too large` in the logs). Either raise `IBLT_CELLS` or accept that the cluster will fall back to full-state exchange for that round.
- If a node that just (re)joined has an empty IBLT for a partition it owns, check the `[ownership]` logs — `EnsurePartition` should have been called and will log if it fails.
- [docs/REVIEW.md](docs/REVIEW.md) is the running design-review log; open items there are the canonical "known limitations" list.
