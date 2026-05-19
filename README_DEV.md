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
  - [Routing (`internal/coordinator`, `internal/hrw`)](#routing-internalcoordinator-internalhrw)
  - [Node and storage (`internal/node`, `internal/storage`)](#node-and-storage-internalnode-internalstorage)
  - [CRDT and HLC (`internal/crdt`, `internal/hlc`)](#crdt-and-hlc-internalcrdt-internalhlc)
  - [Anti-entropy (`internal/iblt`, `internal/syncer`)](#anti-entropy-internaliblt-internalsyncer)
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
make test                                  # full suite, race detector, no cache
go test ./internal/syncer -race -v         # single package
go test -run TestIBLTDecode ./internal/iblt  # single test
```

Many tests spin up real BadgerDB instances in a `t.TempDir()`, and the syncer tests bring up small in-process clusters — they run quickly but expect a few hundred milliseconds of headroom.

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
  server/                 main.go — process entry; wires everything together
  partitiontest/          ad-hoc tool for partition experiments
proto/                    gRPC service definitions (source of truth)
  kv/kv.proto             client-facing KVService (Put/Get/Delete/Status)
  forward/forward.proto   internal ForwardService (peer-to-peer client forwarding)
  replication/replication.proto  SyncService (GetIBLT, PushEntries, PullEntries)
gen/                      generated Go (DO NOT edit; run `make proto`)
internal/
  api/                    KVService + ForwardService handlers; key validation
  coordinator/            HRW routing, forwarding, async push fan-out
  node/                   per-key locks, HLC, Put/Get/Delete/ApplyDelta
  storage/                BadgerDB wrapper, SaveBatch, paginated IterateAll
  crdt/                   FieldEntry, AWLWW merge, WinsOver predicate
  hlc/                    Hybrid Logical Clock (Send/Receive/Seed)
  hrw/                    rendezvous hashing over gossip membership
  iblt/                   general-purpose IBLT (Insert/Delete/Subtract/Decode)
  syncer/                 SyncService server + initiator loop + IBLTState
  gossip/                 memberlist wrapper + changeCh worker
  connpool/               shared gRPC connection pool (evict-on-leave)
  utils/                  small slice helpers
docs/                     mermaid sources + auto-generated SVGs (see workflow)
docker-compose.yml        3-node local cluster
Dockerfile                multi-stage build → /convergekv
Makefile                  proto / build / test / format / docker targets
```

## How the components fit together

### Process bootstrap (`cmd/server`)

[main.go](cmd/server/main.go) is the only place subsystems are wired. The ordering matters and is intentional — see the [bootstrap diagram](#diagrams):

1. Parse env config.
2. Open BadgerDB (`storage.Open`).
3. Scan `store.MaxTimestamp()` and pass it as `node.WithHLCFloor(...)` so the HLC can never go backwards across a restart.
4. Build the IBLT from the persisted store (`syncer.BuildFromStore`) and inject it into the node (`node.SetIBLTState`).
5. Construct the shared `connpool.Pool`.
6. Start gossip — `joinWithRetry` blocks here until at least one seed is reachable, so by the time we register handlers we already have a cluster view.
7. Construct syncer, forwarder, coordinator.
8. Register the three gRPC services and start listening.
9. Kick off `sync.Run(ctx)` in a goroutine.
10. Serve until SIGINT/SIGTERM; then shutdown in reverse order.

### API surface (`internal/api`)

[handler.go](internal/api/handler.go) implements `KVService` (`Put`/`Get`/`Delete`/`Status`). The handler is intentionally thin — it validates the key (`validateKey` rejects `\x00` because the storage layer uses it as the `(key, field)` delimiter) and hands off to the coordinator. `Status` builds the peer list from live gossip membership.

[forward_handler.go](internal/api/forward_handler.go) implements `ForwardService`, the internal RPC the coordinator uses when this node isn't in the HRW set for a key.

### Routing (`internal/coordinator`, `internal/hrw`)

[coordinator.go](internal/coordinator/coordinator.go) decides, per request, whether this node is one of the `RF` HRW replicas:

- If yes, it calls into `node.Put`/`Get`/`Delete`, returns the response synchronously, and then `launchPush` fans the same entries out to the other replicas via `syncer.PushToPeers` (tracked goroutine, 2 s timeout from the coordinator context).
- If no, `forwardWithRetry` tries each replica in HRW-score order and aborts when the request context is cancelled.

[hrw.go](internal/hrw/hrw.go) is stateless: `Replicas(key, members, rf)` is a pure function of the live gossip view, scored by `xxhash(key ∥ replicaID)` with deterministic tie-break on `ReplicaID`.

### Node and storage (`internal/node`, `internal/storage`)

[node.go](internal/node/node.go) owns three things:

- The `HLC` (its own mutex; safe to call without any other lock held).
- The per-key reference-counted `keyLocks` table, used only on the write path. Reads (`GetKey`/`GetField`) hit Badger directly and rely on its MVCC.
- A reference to the `IBLTUpdater` (set once at startup via `SetIBLTState`).

`Put` / `Delete` ([put.go](internal/node/put.go), [delete.go](internal/node/delete.go)) follow the same pattern:

```
acquireKey(key)               // per-key mutex; ref-counted
hlc.Send()                    // inside the lock — order matches persistence
store.GetKey(key)             // read existing fields to compute IBLT removals
store.SaveBatch(updates)      // single atomic db.Update
ibltState.Remove(old) / Insert(new)
return ts, updates
```

`ApplyDelta` ([merge_incoming.go](internal/node/merge_incoming.go)) is the inverse — used by the syncer and push handler. It uses `hlc.Receive` to advance the local clock from a remote timestamp and `crdt.WinsOver` to skip stale entries.

[storage/badger.go](internal/storage/badger.go) is the only file that touches Badger:

- **Key encoding** — `badgerKey` is `key + "\x00" + field`; `decodeKey` splits on the first `\x00` so fields containing `\x00` round-trip correctly.
- **`SaveBatch`** wraps the whole batch in a single `db.Update`; either the whole multi-field write commits or nothing does.
- **`IterateAll`** pages the scan into 1000-record blocks, each in its own short-lived read transaction, so a slow network consumer (full-state pull) doesn't pin Badger's MVCC GC.
- **`MaxTimestamp`** does a one-shot scan at startup to find the highest persisted HLC; the result feeds `node.WithHLCFloor`.

### CRDT and HLC (`internal/crdt`, `internal/hlc`)

`crdt.FieldEntry` is the unit of state: `{Value, Timestamp, ReplicaID, Deleted}`. `crdt.WinsOver(a, b)` compares HLC first and falls back to `ReplicaID` for ties; `merge.go` implements the AWLWW map merge used by `ApplyDelta` and the tests.

`hlc.HLC` is the standard CLOCK-SI Hybrid Logical Clock with three entry points:

- **`Send()`** before originating a local event (write). Rejects with `ErrClockDrift` if the HLC has drifted more than `MAX_CLOCK_DRIFT_MS` (10 min) ahead of wall time.
- **`Receive(remote)`** when applying a remote event. Rejects with the same error if the *remote* timestamp is that far ahead.
- **`Seed(floor)`** at startup, called by `WithHLCFloor`, to raise the clock above whatever's already on disk.

### Anti-entropy (`internal/iblt`, `internal/syncer`)

[iblt.go](internal/iblt/iblt.go) is a general-purpose IBLT. Items are opaque `[]byte`; the IBLT stores three values per cell (`Count`, `KeySum`, `HashSum`) and uses 3 deduplicated hash positions per item (`h1 + i·h2 mod n`, with `h2 |= 1` so the sequence visits `n` distinct slots before repeating). `Decode` peels pure cells iteratively and returns `(onlyInA, onlyInB, ok)`; `ok == false` means the symmetric difference was too large.

[syncer/iblt_state.go](internal/syncer/iblt_state.go) is the IBLT *as seen by the rest of the system*. Each item is the canonical fixed-width serialisation of `(key, field, HLC, replicaID, deleted)`. The node's write path calls `InsertEntry`/`RemoveEntry` on this state object on every `Put`/`Delete`; `BuildFromStore` rebuilds it from Badger at startup so any pre-crash divergence between IBLT and disk heals on restart.

[syncer/syncer.go](internal/syncer/syncer.go) is the initiator of the three-step protocol described in the [sync diagram](#diagrams). Read it together with [syncer/handler.go](internal/syncer/handler.go) (the server side) — both halves are short and the symmetry is the easiest way to reason about the protocol. `SyncWithPeer` also implements the full-state fallback by concurrently `PushEntries`-streaming the entire local store and `PullEntries`-receiving the peer's entire store; both sides iterate Badger lazily via the paginated `IterateAll`.

[`SyncService.PushEntries`](proto/replication/replication.proto) is reused on the write path (`syncer.PushToPeers`, invoked from `coordinator.launchPush`) so there is exactly one wire shape for "apply these entries to that peer".

### Gossip and connection pool (`internal/gossip`, `internal/connpool`)

[gossip.go](internal/gossip/gossip.go) wraps HashiCorp memberlist. The non-obvious bit is the `changeCh` + `changeWorker` pattern: memberlist invokes event callbacks while holding its own lock, so the eventDelegate just signals a buffered channel and a dedicated goroutine rebuilds `g.members` and fires `onChange` *outside* the memberlist lock. This is what makes it safe for any consumer (HRW, syncer, the `Status` handler) to call back into `gossip.Members()` from arbitrary goroutines.

`joinWithRetry` blocks `Start` until the first successful join with exponential backoff capped at 30 s — so a node never starts serving requests without a cluster view.

[connpool/pool.go](internal/connpool/pool.go) is a tiny `map[string]*grpc.ClientConn` keyed by `host:port`. `EvictAbsent(keep)` snapshots the to-close connections under the mutex, releases it, *then* calls `Close()` — so a slow `Close()` cannot stall concurrent `Get` callers. `main.go` wraps the per-`OnChange` eviction in `go pool.EvictAbsent(keep)` so the notification path itself can't block either.

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

- The storage key encoding uses `\x00` as the `(key, field)` delimiter. **Client-supplied** keys are rejected by `api.validateKey`. Peer-supplied keys arriving on `SyncService` (`PushEntries`, `PullEntries`) are not currently validated — see `N1` in [docs/REVIEW.md](docs/REVIEW.md).
- Every `DeltaEntry` carries the full `(key, field, value, HLC, replicaID, deleted)` tuple. Tombstones are normal entries with `Deleted=true` and are persisted, IBLT-tracked, and replicated identically to live values.
- IBLT items use a fixed-width binary encoding ([iblt_state.go](internal/syncer/iblt_state.go) `serialiseItem`); changing it requires bumping every node simultaneously (no version negotiation exists yet).

## Adding or changing an RPC

1. Edit the relevant `.proto` under [proto/](proto/).
2. Run `make proto` to regenerate `gen/`.
3. Update the server-side handler:
   - Client API → `internal/api/handler.go`
   - Internal forwarding → `internal/api/forward_handler.go`
   - Anti-entropy / replication → `internal/syncer/handler.go`
4. Update the client-side caller (the coordinator's forwarder for `ForwardService`; the syncer's loop for `SyncService`).
5. Add a focused test in the package that owns the handler. The syncer tests are a good template for end-to-end coverage; the storage tests are a good template for round-trip encoding coverage.

## Debugging tips

- All log lines are prefixed with the originating subsystem in square brackets (`[gossip]`, `[syncer]`, `[coordinator]`, `[hlc]`, etc.). Filter with `grep` rather than reading the full stream.
- `Status` RPC returns the live gossip view as seen by the node you query — useful to confirm membership before suspecting replication.
- An `ErrClockDrift` log line from `[syncer]` means a peer is more than 10 minutes off wall time. Fix the clock; do not raise `MAX_CLOCK_DRIFT_MS` ([hlc.go](internal/hlc/hlc.go)).
- If anti-entropy looks stuck, check whether `Decode` is failing (look for `IBLT diff too large` in the logs). Either raise `IBLT_CELLS` or accept that the cluster will fall back to full-state exchange for that round.
- [docs/REVIEW.md](docs/REVIEW.md) is the running design-review log; open items there are the canonical "known limitations" list.
