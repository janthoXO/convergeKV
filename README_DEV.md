# convergeKV — Developer Guide

This document is for developers who want to understand how convergeKV works
internally or contribute to it. For a product/user-level overview, see
[`README.md`](README.md). For the full design rationale (the "why" behind each
decision), see the chapter-by-chapter docs in [`docs/concepts/`](docs/concepts/);
this guide is self-contained and does not depend on them.

- [System summary](#system-summary)
- [The load-bearing design decisions](#the-load-bearing-design-decisions)
- [Repository layout](#repository-layout)
- [Package architecture & the dependency rule](#package-architecture--the-dependency-rule)
- [The CRDT core](#the-crdt-core)
- [Request paths](#request-paths)
- [Replication & anti-entropy](#replication--anti-entropy)
- [Placement & membership](#placement--membership)
- [Storage](#storage)
- [Node lifecycle & crash recovery](#node-lifecycle--crash-recovery)
- [Wire protocol (gRPC)](#wire-protocol-grpc)
- [Configuration & invariants](#configuration--invariants)
- [Building, testing, and tooling](#building-testing-and-tooling)
- [Testing strategy](#testing-strategy)
- [Regenerating protobuf](#regenerating-protobuf)
- [CI](#ci)
- [Contributing](#contributing)

---

## System summary

convergeKV is a distributed, **AP** key-value store with **strong eventual
consistency**: any two replicas that have observed the same set of updates are in
the same state, regardless of delivery order or duplication. It achieves this
with a **causal δ-CRDT** document model. Every node runs identical code and is a
peer; there is no coordinator node and no quorum on the write path.

- Language: **Go 1.26**, module `github.com/janthoXO/convergeKV`.
- Storage: **Pebble** (embedded LSM, from CockroachDB).
- Membership: **SWIM gossip** via `hashicorp/memberlist`.
- RPC: **gRPC / protobuf**.
- Binary: `cmd/kvnode` → `dist/kvnode`.

## The load-bearing design decisions

These are deliberate and central. Understand them before changing behavior.

- **No quorums.** A write succeeds once *one* owner (the **applier**) persists it
  durably (one `fsync`, no peer round-trip). Replication to the other owners is
  asynchronous and fire-and-forget.
- **Anti-entropy is the *only* repair mechanism.** There is no read repair and no
  acknowledged replication buffer. A periodic Merkle-tree comparison between
  owners is the sole thing that heals divergence — which is exactly why
  replication can afford to be lossy.
- **HRW (rendezvous) placement.** `partition = xxhash(key) % P`; each partition is
  owned by the top **RF = 3** nodes a shared hash ranks highest. Every node
  computes ownership locally and identically — no placement coordinator. `P` is
  fixed at cluster birth (power of two ≤ 1024, default 256) and never changes.
- **HLC (Hybrid Logical Clocks)** break last-writer-wins ties.
- **Atomic doc+leaf batches.** Each document and its Merkle leaf are written to
  Pebble in one atomic batch, so the anti-entropy fingerprint never drifts from
  the data.

## Repository layout

```
cmd/kvnode/         Thin binary: load config, start node, wait for signal, stop.
internal/           All implementation packages (see next section).
pkg/proto/          gRPC/protobuf wire definitions + generated code.
test/cluster/       In-process multi-node integration/chaos/bench/leak harness.
docs/concepts/      Deep design docs (11 chapters).
docs/bruno/         Bruno gRPC request collection.
docs/BENCHMARKS.md  Benchmark methodology and results.
deploy/             Prometheus + Grafana provisioning.
docker-compose.yml  Seed + scalable peers (+ optional monitoring profile).
Dockerfile          Multi-stage static build on Alpine.
Makefile            build / test / lint / format / proto / docker targets.
```

## Package architecture & the dependency rule

Dependencies point **downward**: pure logic at the bottom, I/O and wiring at the
top. The CRDT core can be tested exhaustively with no network or disk.

![Package dependency graph: dependencies point downward to the pure CRDT core](docs/readme/packages.svg)

| Package | Responsibility |
|---|---|
| `internal/crdt` | **Pure CRDT core**: documents, causal context (version vector + cloud), OR-Map fields, LWW registers, dots, merge/join. **Imports the Go standard library only.** |
| `internal/coordinator` | Request handling: Put/Patch/Get/Delete, routing, per-partition locking, delta merge. The same code runs on every node — "coordinator" is a per-request role, not a node. |
| `internal/placement` | HRW ownership computation; the immutable owners `View`. |
| `internal/cluster` | Gossip membership (`memberlist`) and per-node metadata. |
| `internal/storage` | Pebble wrapper: atomic doc+leaf batches, ownership bitmap, liveness lease, GC state, partition/bucket scans. |
| `internal/merkle` | Merkle hashing primitives (leaves, root). |
| `internal/codec` | JSON ↔ CRDT boundary (`SplitFields`, `RenderDocument`). |
| `internal/replication` | Async per-peer fan-out (bounded queue, drops under overload/age). |
| `internal/antientropy` | The Merkle repair engine. |
| `internal/transfer` | Bootstrap data movement on churn (snapshots). |
| `internal/gc` | Tombstone / causal-context garbage collection. |
| `internal/hlc` | Hybrid Logical Clock. |
| `internal/identity` | Node UUID (= CRDT ActorID), persisted, atomically written. |
| `internal/nodeid` | Node-ID type alias and helpers. |
| `internal/config` | Env-var config loading and validation. |
| `internal/metrics` | Prometheus collectors + pprof admin endpoint. |
| `internal/api` | The gRPC servers (`KV`, `Node`, `Debug`) + the peer connection `Pool`. |
| `internal/node` | Assembles everything: ordered startup, background loops, lifecycle. |

> **The one rule to never break:** `internal/crdt` imports nothing outside the
> standard library (enforced by `internal/crdt/stdlib_test.go` and the header
> comment in `types.go`). That purity is what lets it be property- and
> fuzz-tested with randomized operation sequences.

## The CRDT core

A **document** is the value stored for one key: a map of top-level fields plus a
**causal context**.

- **Dot** — a globally unique write event id `(ActorID, Seq)` ("the N-th write by
  actor X"). `ActorID` is the 16 raw bytes of the node's UUID.
- **Register** — one field's value plus the dot and HLC that produced it. Field
  values are **opaque bytes**, kept verbatim (see `internal/codec`); there is no
  recursive merge inside a field.
- **OR-Map semantics** — a field is present if it has at least one observed,
  un-removed dot. This is what makes "concurrent edit to a different field" safe.
- **Causal context** — the set of *all* dots a replica has observed for a
  document, stored compactly as a **version vector** (per-actor highest
  contiguous seq) plus a **cloud** (out-of-order dots not yet contiguous). This
  is what makes deletes safe: a removed value cannot be resurrected by a
  re-delivered older write, because its dot is already "seen."
- **Merge (join)** is commutative, associative, and idempotent — the property
  tests verify these laws.
- **LWW** resolves two concurrent values of the same field by higher HLC, ties
  broken deterministically.

A **delete** clears all fields but **retains the causal context** — a residual
tombstone that reads as not-found and cannot be undone by a late write. GC
reclaims residuals only after a two-clean-round anti-entropy certification.

**Encoding:** documents have a canonical byte encoding (`document.go` /
`encode.go` / `decode.go`) used for storage, replication, and idempotency
detection (a re-delivered delta that doesn't change the canonical bytes skips the
disk write).

## Request paths

All in `internal/coordinator/coordinator.go`; the same code runs everywhere.

**Put / Patch / Delete (write path):**

1. The receiving node parses the request — for `Put`/`Patch` it splits the JSON
   into fields (`codec.SplitFields`); `Put` rejects anything that isn't a
   non-empty object, `Patch` allows an empty `value` when it only deletes
   fields — **before** minting anything.
2. It computes the partition and routes to the **applier** — the first
   serving owner in HRW rank order. If the receiving node is that owner, it runs
   locally (no hop); otherwise it `Forward`s the whole request (one hop).
3. The applier **re-checks eligibility** (`CheckWriteEligible`) because the
   sender's membership view may be stale.
4. Under the **per-partition lock**, it loads the doc, mints dot(s), applies the
   mutation (producing one combined delta), and persists the doc + Merkle leaf in
   **one synced atomic batch**.
5. It acknowledges the client — **no quorum**, no peer round-trip.
6. It enqueues the delta for asynchronous fan-out to the other owners.

`Put`, `Patch`, and `Delete` share one core (`Coordinator.applyWrite`):
upsert a set of fields and remove a set of fields under the lock, as one delta.

- **`Put` is replace:** the remove set is *every observed field the request
  omits*, computed from the loaded doc.
- **`Patch` is partial:** the remove set is the explicit `delete_fields`.
- Both removals are **add-wins** — `RemoveField` only covers dots the applier has
  already observed, so an unobserved concurrent add survives (no hard
  whole-document replace under concurrency). Removing a field the applier doesn't
  hold is skipped, so it never adds pointless tombstone context.
- **`Delete`** removes the whole document, leaving a residual context (below).

**Get (read path):** served from **one** active owner, locally if possible
(`ReadLocal`), otherwise forwarded. **No read repair** — a read returns whatever
the chosen owner holds; divergence is healed only by anti-entropy.

**Concurrency model:** one `sync.Mutex` per partition serializes read-modify-write
for that partition. There is **no global lock** — writes to different partitions
run fully in parallel ("shard-per-partition").

## Replication & anti-entropy

**Fan-out (`internal/replication/fanout.go`):** one bounded queue + one worker
goroutine per peer. `Enqueue` never blocks: if a peer's queue is full, the delta
is **dropped and counted** (load shedding). Each delta is retried with
exponential backoff and given up after `MaxAttempts` *or* once older than
`MaxAge`. Whatever is dropped is the anti-entropy backstop's problem.

**Receiving a delta (`Coordinator.MergeDelta`):** fold the delta's max HLC into
the local clock (HLC receive rule), merge under the partition lock, and skip the
disk write if canonical bytes are unchanged (idempotent redelivery). **Owners
never refuse a delta on eligibility grounds** — refusing one would permanently
desync that owner. The single exception is the GC "absorbing rule."

**Anti-entropy (`internal/antientropy/engine.go`):** per partition, on a jittered
interval, owners compare Merkle roots; only if roots differ do they fetch the
leaf vector, and only for differing buckets do they stream the actual documents
(`SyncBucket`). Steady-state cost is near-zero (leaf fetches stay at 0 when
clean). This is the only repair mechanism, so it must run aggressively.

## Placement & membership

**Placement (`internal/placement`):** `Compute` builds an immutable owners
`View` from a membership snapshot. For each partition, every alive node (plus
dead-within-grace "phantoms" that still hold their slots) is ranked by
`xxhash64(nodeID ‖ partitionID)`; the top `RF` are owners. Because it's a pure
function of the view, every node derives an identical table. Draining owners
extend the list past `RF` so a successor can bootstrap before the leaver goes.
Key sets: `WriteSet` (active+bootstrapping+draining), `ReadSet` (active+draining),
`Applier` (first serving owner).

**Membership (`internal/cluster`):** wraps `memberlist`. Each node gossips a
`NodeMeta` (id, partition count, generation, RPC address, and per-partition
status flags packed 4-per-byte). The 512-byte metadata cap is why `P ≤ 1024`.
A coalescing `Changed()` signal drives the node's reconciliation loop.

## Storage

`internal/storage` wraps Pebble. The defining property: a document and its Merkle
leaf are updated in **one atomic batch** (`persist`), so the fingerprint can never
disagree with the data even across a crash. The store also holds the **ownership
bitmap** (`owned`), the **HLC checkpoint**, the **liveness lease**
(`last_alive`), and **GC counters**. It is bound to the node's UUID and partition
count at open time and rejects a foreign data directory. Scans (`ScanPartition`,
`ScanBucket`) run over consistent snapshots and back replication/transfer and the
debug dump.

## Node lifecycle & crash recovery

`internal/node/node.go` assembles a node with an ordered startup behind a
**deferred cleanup stack** that unwinds on any partial failure (no leaked ports,
handles, or goroutines):

1. Open listeners (port `:0` lets the OS pick; the node reads back the real port).
2. Load/create identity.
3. Open Pebble; bind node-id + partition count.
4. **Lease check** — the crash-recovery gate (below).
5. Restore HLC (`max(checkpoint, wall) + bump`).
6. Join gossip.
7. Wire coordinator, fan-out, anti-entropy, transfer, GC.
8. Compute initial `View`.
9. Start background loops (membership watch, HLC/lease checkpoint, anti-entropy).
10. Start gRPC servers; optionally the admin endpoint.

**The liveness lease (crash-recovery gate):** a node periodically writes
`last_alive`. On restart, the grace period decides whether it resumes cheaply or
must forget everything and rejoin as a stranger:

![Crash-recovery decision: restart within grace keeps data; beyond grace wipes and rotates identity](docs/readme/lifecycle.svg)

On restart:

- **Within `CrashGracePeriod`** → keep data + identity, resume owned partitions
  via the persisted ownership bitmap (no transfer storm); anti-entropy closes any
  small gap.
- **Beyond the grace period** → **`WipeData()` *and* `Rotate()` the UUID
  together**, then rejoin as a stranger. This pairing is non-negotiable: a node
  gone too long may otherwise resurrect cluster-wide-GC'd deletes and bloat
  causal contexts under a retired actor id.

**Shutdown** (`Stop(graceful)`): graceful drains owned partitions (preserving RF),
announces a gossip leave, then stops servers and joins all goroutines **before**
closing the store (handlers and loops touch it). A crash (`graceful=false`, used
by the test harness) just vanishes; peers detect the silence via SWIM.

## Wire protocol (gRPC)

Definitions in [`pkg/proto`](pkg/proto/). Three services:

**`KV`** (client-facing, served on `CLIENT_ADDR`):

| RPC | Message | Notes |
|---|---|---|
| `Put` | `PutRequest{ key, value }` | Replace; `value` = bytes of a non-empty JSON object. Omitted fields are removed (add-wins). |
| `Patch` | `PatchRequest{ key, value, delete_fields }` | Partial update; sets `value`'s fields and removes `delete_fields`. `value` may be empty. A field may not be in both. |
| `Get` | `GetRequest{ key }` → `GetResponse{ found, value, context_hash }` | `value` is the rendered JSON. |
| `Delete` | `DeleteRequest{ key }` | Removes the whole document. |

**`Node`** (peer-to-peer, served on `NODE_ADDR`): `Forward`, `ApplyDelta`,
`MerkleRoot`, `MerkleLeaves`, `SyncBucket` (stream), `Snapshot` (stream). The
streaming RPCs opt into zstd compression; unary RPCs stay uncompressed. Errors
map to gRPC codes at the boundary (`toStatus`): `Unavailable` (no owner
reachable, retry), `FailedPrecondition` (stale view, retry elsewhere),
`InvalidArgument` (bad document), `Internal` (otherwise).

**`Debug`** (introspection, registered on the client port): read-only tooling.

| RPC | Returns |
|---|---|
| `Inspect` | This node's id/generation/partition count/addresses, the membership it sees, and the full per-partition owners table. |
| `DumpDocuments` (stream) | Every document the node physically holds, rendered to JSON, with tombstones flagged. |

`Inspect` reflects one node's *local* view; diffing it across nodes reveals view
divergence during a partition. The peer connection `Pool` (`internal/api/pool.go`)
keeps one lazy client connection per peer and evicts departed peers on membership
change.

> **`bytes` encoding gotcha:** `value` is a protobuf `bytes` field. Native gRPC
> clients pass raw bytes (e.g. `Buffer.from(JSON.stringify(obj))` in Node.js). In
> JSON-based tools (grpcurl, Bruno, Postman) a `bytes` field must be a **base64
> string**, and those tools cache parsed `.proto` descriptors — reload them after
> changing a proto.

## Configuration & invariants

Config is environment-only (`CONVERGEKV_*`, see [`README.md`](README.md#configuration)),
parsed in `internal/config/config.go`: `Default()` → overlay env (`Load()`) →
`validate()`. Hard constraints enforced at startup:

- `PARTITIONS` is a power of two and ≤ 1024.
- **`REPLICATION_MAX_AGE < ANTI_ENTROPY_INTERVAL / 2`.** A stale delta must never
  outlive the GC certification that decided a residual was safe to drop, or it
  could resurrect deleted data.

Other invariants worth internalizing before editing:

- Empty documents are rejected before minting (an empty doc would diverge forever
  and churn anti-entropy).
- A delete leaves a residual context; GC reclaims it only after a two-clean-round
  certification.
- Owners never refuse deltas on eligibility grounds (the GC absorbing rule is the
  one drop case).
- Appliers re-check eligibility on the receive side of a `Forward`.
- Lease expiry ⇒ wipe **and** rotate identity, together.

## Building, testing, and tooling

```bash
make build        # go build -o dist/ ./cmd/...
make test         # go test ./... -race -count=1   (always race + no cache)
make lint         # go vet ./... && golangci-lint run
make format       # gofmt -s -w .
make proto        # regenerate pkg/proto/*.pb.go (needs protoc + plugins)

make docker-up N=3   # docker compose, scaled to N nodes
make docker-down

# Run a single test / package:
go test ./internal/crdt/ -run TestPropertyConvergence -race -count=1 -v
go test ./test/cluster/ -run TestWriteSucceedsWithTwoOfThreeOwnersDown -race -count=1 -v
```

Always run with `-race -count=1` (the Makefile does); concurrency correctness is
a primary concern, and `-count=1` disables the test cache.

## Testing strategy

- **`internal/crdt`** — property tests (`pgregory.net/rapid`) and fuzz tests
  verify the merge laws (commutativity, associativity, idempotence), convergence
  over randomized operation sequences, and bounded context/cloud growth. Any CRDT
  change must keep these green. `stdlib_test.go` enforces the stdlib-only rule.
- **`test/cluster`** — an in-process multi-node harness (`harness.go`) runs many
  real nodes in one process on `:0` ports with `Partitions = 16` (fast
  convergence, same code paths). Suites:
  - `m5`–`m8_test.go` — integration tests, named after milestones (writes,
    failure tolerance, anti-entropy, transfer, GC).
  - `chaos_test.go` — randomized kill/restart churn.
  - `bench_test.go` — latency benchmarks (`docs/BENCHMARKS.md`).
  - `leak_test.go` — zero goroutine leaks via `go.uber.org/goleak`.
  - Hard crashes are modeled with `node.Stop(false)`.

## Regenerating protobuf

`make proto` runs `protoc` over `pkg/proto/*.proto` with `paths=source_relative`.
You need `protoc` plus the Go plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

The generated `*.pb.go` / `*_grpc.pb.go` files **are committed**. After changing a
`.proto`, run `make proto`, update any affected Go references, then `make build`
and `make test`.

## CI

`.github/workflows/build.yml` runs, in order: `gofmt -s -l` (output must be
empty), `make lint`, `make build`, `make test`. **Formatting failures fail the
build** — run `make format` before pushing and match this pipeline locally.

## Contributing

1. Read the relevant `docs/concepts/` chapter for the subsystem you're touching —
   it explains *why* the design is the way it is.
2. Respect the invariants above; many are subtle and safety-critical (delete
   safety, lease/identity pairing, the AE-vs-MaxAge bound).
3. Never add a non-stdlib import to `internal/crdt`.
4. Keep `-race` clean; add or update property/cluster tests for behavioral
   changes.
5. Run `make format && make lint && make build && make test` before opening a PR.
