# Implementation Plan: Leaderless Distributed CRDT Key-Value Store (Go)

A leaderless, multi-writer, sharded KV store. RF=3, fixed partition count, HRW placement,
gossip membership, delta-CRDT replication over JSON documents (top-level field-wise
merge, nested values opaque), Merkle anti-entropy,
Pebble storage.

This document is a handoff spec. Section 1 lists decisions that are **final** — do not
revisit them. Section 2 defines the exact CRDT semantics. Section 3 is the package layout.
Section 4 is the milestone plan with acceptance criteria. Section 5 lists invariants that
must be encoded as property tests. Section 6 lists known deferrals.

---

## 1. Decisions (final — do not redesign)

| Area | Decision |
|---|---|
| Partitioning | Fixed partition count `P` (default 256, power of two), set at cluster bootstrap, gossiped; a joining node with a different `P` is rejected and shuts down with a clear error. `partition(key) = xxhash64(key) % P`. |
| Placement | HRW (rendezvous hashing) **over partitions, not keys**: for each partition, rank all alive nodes by `xxhash64(nodeID ‖ partitionID)`, owners = top 3. Recomputed on every membership change, cached. |
| Transition handling | Per-partition, per-node status flag gossiped: `bootstrapping` / `active` / `draining`. A `bootstrapping` owner receives writes/deltas but does not serve reads and is never chosen as applier. |
| Membership | SWIM gossip via `hashicorp/memberlist`. Node identity = UUID generated once and persisted to disk; never derived from IP. |
| Coordination | Every node runs identical coordinator logic. Non-owner nodes forward the whole request to one owner (one extra hop). **No quorums.** A write succeeds once the applier has applied and persisted it locally; replication to the other owners is asynchronous fire-and-forget with bounded retry. Reads are served by a single active owner (locally, if the receiving node is one). No read repair — Merkle anti-entropy is the sole repair mechanism. |
| Hinted handoff | **Not implemented.** A write fails (retryable `UNAVAILABLE`) only when no healthy owner is reachable to act as applier. |
| Value model | Any JSON object is accepted. Merge granularity is the **top level only**: each top-level field is an independent register. A field whose value is a scalar, an array, **or a nested object** is stored as one opaque value and resolved LWW as a whole — no merging inside nested objects or arrays. No counters. |
| CRDT | Causal CRDT: per-document causal context (dots), OR-Map of fields, LWW-over-HLC registers at leaves. Exact semantics in section 2. |
| Dot minting | The coordinator forwards the client op to the **first healthy owner in HRW rank order** (the "applier"). The applier mints the dot, applies and persists locally, responds to the client, then fans the delta to the other owners asynchronously. Only owners ever mint dots. |
| Clocks | Hybrid Logical Clock per node: 48-bit physical ms + 16-bit logical counter, packed into uint64. Updated on every local event and every received delta (standard HLC receive rule). |
| Replication | Foreground delta replication (applier → other owners), fire-and-forget through a bounded **in-memory** retry queue (exponential backoff, capped attempts and age, drop on overflow). Merkle anti-entropy is the durable backstop for anything dropped. No persistent per-peer delta buffers, no acks. |
| Anti-entropy | One incrementally-maintained Merkle tree per (partition, local node). Leaf hash = hash of the document's **causal context** (not the value). Periodic exchange between the 3 owners of each partition, jittered, rate-limited. |
| GC | No per-field tombstones (structural, via dot store). Two pieces of explicit logic only: (a) deleted-document residual contexts dropped after 2 consecutive clean Merkle rounds covering that key; (b) dead actor IDs retired from contexts after the re-replication grace period, verified during anti-entropy. |
| Storage | Pebble. Key layout in section 3.4. All related writes (document + Merkle leaf) in one atomic `pebble.Batch`. |
| Wire | gRPC + protobuf. Unary: client API, ApplyDelta. Server-streaming: partition transfer, Merkle exchange. zstd on streams only. |
| Concurrency | Shard-per-partition: a striped lock (or single-goroutine loop) per locally-owned partition serializes read-modify-write per partition. No global locks on the data path. Every queue bounded; overflow → retryable `RESOURCE_EXHAUSTED`. |

---

## 2. CRDT semantics (exact — implement precisely as written)

> **CORRECTION (M1, 2026-06-11) — the semantics below as originally written are
> not convergent; `internal/crdt` deviates deliberately. Do not "fix" the code
> back to this text.** Property testing (`TestPropertyConvergence`) found two
> counterexamples:
> 1. A put delta whose context is `{only dot d}` lets a peer that received the
>    field's remove before a stale put resurrect the stale register. → A put
>    delta's context must also carry the dots of the registers it replaces.
> 2. Collapsing concurrent registers to a single LWW winner **at merge time**
>    loses the loser's dot: it is recorded only in the origin's full context,
>    which deltas never carry, so a peer that adopts the loser after seeing the
>    field's removal keeps it forever.
>
> The implemented model is the standard causal δ-ORMap with **multi-value
> leaves**: `Fields map[string][]Register` keeps every concurrent register;
> merge is the dotted-store join (a register survives iff present on both
> sides, or present on one side and not covered by the other's context); LWW
> arbitration (HLC, then actor bytes, then seq) happens **at read time** via
> `Document.Get`. Any later put covers the field's whole register set, so
> concurrency collapses on the next write. Client-visible semantics are
> unchanged from this section's intent: one whole-value winner per field, no
> partial mixing, no resurrection.
>
> **Follow-on corrections required by the above:**
> - *Merkle leaf hash (§1 anti-entropy row):* leaves hash the **full
>   canonical document**, not just the causal context. With multi-value
>   register sets, replicas can hold identical contexts with different
>   register subsets; a context-only hash makes that divergence permanently
>   invisible to anti-entropy (found by the chaos suite).
> - *Actor retirement (§1 GC row):* a node whose liveness lease expired
>   (dead past the crash grace period) wipes its data AND **rotates its
>   actor identity** on restart. Its old actor may have been retired from
>   contexts cluster-wide; if it minted new dots, peers could never compact
>   them (the retired prefix is gone forever) and clouds would grow without
>   bound.

### 2.1 Types

```go
type ActorID = [16]byte        // node UUID
type Dot struct { Actor ActorID; Seq uint64 }
type HLC = uint64              // 48-bit physical ms << 16 | 16-bit logical

// CausalContext: everything this replica has ever observed for this document.
type CausalContext struct {
    VV    map[ActorID]uint64   // contiguous: all seqs 1..VV[a] seen
    Cloud map[Dot]struct{}     // out-of-order dots; compacted into VV when gaps fill
}

type Register struct {
    Dot   Dot                  // the write event that produced this value
    HLC   HLC                  // assignment timestamp for LWW arbitration
    Value []byte               // encoded scalar, whole array, or whole nested object (all opaque)
}

type Document struct {
    Fields  map[string]Register // present fields only — absence + context = removed
    Context CausalContext
}
```

`Context.Contains(d Dot) = d.Seq <= VV[d.Actor] || d ∈ Cloud`.
`Context.Add(d)` inserts into Cloud then compacts: while `VV[a]+1 ∈ Cloud`, increment VV, remove from Cloud.

### 2.2 Local operations (executed only by the applier, under the partition lock)

**Put(field, value):** mint `d = (self, ++seq[self])`; `hlc = hlcNow()`;
`Fields[field] = Register{d, hlc, value}`; `Context.Add(d)`.
Delta = `Document{ Fields: {field: that register}, Context: {only dot d} }`.

**RemoveField(field):** mint dot `d` (the remove is itself an event); delete `Fields[field]`;
`Context.Add(d)`. Delta = `Document{ Fields: {}, Context: {d} ∪ {dot of the removed register} }`.
(The removed register's old dot MUST be in the delta context — that is what communicates the removal.)

**DeleteDocument:** mint dot `d`; clear `Fields`;
delta context = `{d} ∪ {dots of all removed registers}`. Local state becomes
`Fields = {}` with full Context retained (residual context — see GC).

**Dot granularity rule: one dot per field mutation.** A multi-field client write is a
single delta containing one freshly minted dot per mutated field. (Do not share one dot
across fields — fields are removed independently later, and removal is communicated per
dot, so dots must be per-field.)

### 2.3 Merge (the join — used for replica deltas, read-path merging, anti-entropy, transfer)

`merge(local, remote) -> local'` over documents:

```
for each field f in local.Fields ∪ remote.Fields:
    L = local.Fields[f]   (may be absent)
    R = remote.Fields[f]  (may be absent)
    case L present, R absent:
        if remote.Context.Contains(L.Dot): drop f      // remote saw it and removed it
        else: keep L                                    // remote never saw it
    case L absent, R present:
        if local.Context.Contains(R.Dot): stay absent   // we removed it
        else: Fields[f] = R                             // new to us
    case both present:
        if L.Dot == R.Dot: keep L                       // same write
        elif local.Context.Contains(R.Dot): keep L      // R is old news we superseded
        elif remote.Context.Contains(L.Dot): Fields[f] = R   // L superseded remotely
        else:                                           // true concurrency: LWW
            winner = higher (HLC, then bytes-compare of Actor as tiebreak)
            Fields[f] = winner
            // loser's dot still enters the context below — it can never resurrect
local.Context = union(local.Context, remote.Context)    // VV pointwise max + cloud union + compact
```

Merge MUST be commutative, associative, idempotent (property-tested, section 5).

### 2.4 Read result

A read returns the merged `Fields` rendered back to a JSON object (nested values emitted
verbatim from their opaque bytes), plus an opaque
`context_hash` (for diagnostics). Empty `Fields` with non-empty context = "not found".

---

## 3. Repository layout

```
cmd/kvnode/              main: config load, wiring, lifecycle
internal/identity/       node UUID persistence, ActorID
internal/hlc/            hybrid logical clock
internal/crdt/           Dot, CausalContext, Register, Document, merge, deltas (PURE — no I/O)
internal/codec/          protobuf <-> crdt types; JSON boundary: split top-level fields, keep nested values opaque
internal/storage/        pebble wrapper: docs, merkle nodes, delta logs, meta; atomic batches
internal/cluster/        memberlist wrapper, gossip metadata (P, status flags), event bus
internal/placement/      HRW over partitions, owner lookup, status-flag view, transition logic
internal/coordinator/    client request handling, forwarding to applier / active owner
internal/replication/    applier async fan-out, bounded in-memory retry queue
internal/antientropy/    incremental merkle trees, exchange protocol, repair, clean-round bookkeeping
internal/transfer/       partition bootstrap streaming (snapshot + live delta overlap)
internal/gc/             residual-context reaper, dead-actor retirement
internal/api/            gRPC server (client-facing + node-facing services)
internal/metrics/        prometheus registry, named metrics (section 4, M9)
pkg/proto/               .proto files + generated code
test/cluster/            multi-node in-process harness, fault injection, property/chaos tests
```

### 3.4 Pebble key layout (single DB, prefix-separated)

```
'd' ‖ partitionID(uint16 BE) ‖ userKey      -> encoded Document
'm' ‖ partitionID ‖ treePath                -> merkle node hash
'g' ‖ partitionID ‖ userKey                 -> residual-context GC bookkeeping (clean-round counter)
'x' ‖ name                                  -> node meta (uuid, P, hlc checkpoint, dot seq checkpoint)
```

Dot sequence counter (`seq[self]`) is persisted in the same batch as every applied op
(crash must never reuse a dot). HLC is checkpointed periodically; on restart,
`hlc = max(checkpoint, wallclock) + safety bump`.

---

## 4. Milestones

Each milestone must compile, pass `go vet` + `golangci-lint`, and pass its acceptance
tests before the next begins. Keep `internal/crdt` free of any import outside stdlib.

### M0 — Skeleton
Module init, config (YAML + env), structured logging (slog), identity persistence,
graceful shutdown plumbing, Makefile, CI (test + lint + race).
**Accept:** node starts, creates/loads UUID, shuts down cleanly; `-race` clean.

### M1 — CRDT core (pure)
`internal/hlc`, `internal/crdt` per section 2. Delta generation for Put/RemoveField/Delete.
Context compaction. Deterministic binary encoding of Context for hashing.
**Accept:** unit tests for every rule in 2.3; property tests from section 5 pass with
10k randomized op sequences across 3 simulated replicas; fuzz test on merge does not panic.

### M2 — Storage
Pebble wrapper, key layout 3.4, codecs, atomic batch combining doc + merkle leaf,
snapshot range-scan API per partition, dot-seq + HLC checkpointing, crash-restart test.
**Accept:** kill -9 mid-write-storm, restart: no dot reuse, no torn documents (verified by
checksum scan); range scan of a partition returns exactly its keys.

### M3 — Membership
memberlist integration; gossip payload: partition count P, per-partition status flags,
node generation. P-mismatch rejection. Event bus (join/leave/suspect/dead/update) consumed
by placement. Configurable seeds.
**Accept:** 5-node in-process cluster converges < 2s; node with wrong P is rejected and
exits; kill a node → others see `dead` within failure-detector bounds.

### M4 — Placement
HRW over partitions; `Owners(partition) []NodeID` ranked; recompute on membership events;
status-flag view merged from gossip; `EffectiveReadSet` (active owners) vs `WriteSet`
(active + bootstrapping owners).
**Accept:** deterministic across nodes (golden test: same membership → identical tables);
adding 1 node to 9 moves ≈ P·3/10 partition-replicas (±20%); bootstrapping owners excluded
from read sets.

### M5 — RPC + coordinator (first end-to-end writes)
Proto definitions; client service (Get/Put/Delete); node service (Forward, ApplyDelta,
ReadLocal); applier logic per section 1: mint dots, apply + persist locally, respond to the
client, then fan the delta to the other owners asynchronously through a bounded in-memory
retry queue (exponential backoff, capped attempts/age, drop on overflow with a metric —
the Merkle backstop owns whatever is dropped). Read path: serve from one active owner
(locally if the receiving node is one, otherwise forward to the first healthy active owner).
Per-partition striped locking around read-modify-write.
**Accept:** 3-node cluster: a write via a non-owner returns success after applier persist
and appears on all owners eventually (< 1s bound under healthy network in the test harness);
writes still succeed with 2 of 3 owners down; concurrent writes to different fields of one
key from two clients both survive on all replicas; concurrent writes to the same field
converge to one value on all replicas; concurrent writes to the *same top-level field
holding a nested object* converge to exactly one side's whole object (no partial mixing);
retry queue drains after a brief injected peer
outage; an injected queue overflow increments the drop metric (and is healed later by M6).

### M6 — Merkle anti-entropy
Incremental tree per partition (fixed-depth, e.g. 2^10 leaf buckets; leaf = XOR/Blake3 of
context-hashes of keys in bucket — updated in the same batch as the write). Exchange:
root compare → descend differing branches → exchange diverging keys' full documents →
merge both sides. Jittered schedule, token-bucket rate limit, per-round metrics
(`keys_repaired`, `clean_rounds`). Because there is no read repair and no persistent delta
buffer, AE is the **only** repair mechanism — the default interval must be aggressive
(30–60s, jittered) and configurable; staleness windows are bounded by it.
**Accept:** silently corrupt/lag one replica's partition; next AE round restores byte-equal
contexts on all 3; steady-state rounds report zero repairs; AE traffic for a clean round
is O(tree depth), not O(keys).

### M7 — Join, departure, crash (transfer)
Join: gossip `bootstrapping` for gained partitions → stream snapshot from highest-ranked
active owner (pebble snapshot, chunked, zstd) while concurrently receiving live deltas
(merge handles overlap — no sequencing) → flip to `active`. Planned leave: `draining`,
ensure successors active, then exit. Crash: after configurable grace period (default 10m),
placement promotes next HRW node, which bootstraps as above.
**Accept:** scale 3→5 nodes under load: zero failed reads/writes, all partitions end
`active`, ownership counts balanced; kill a node permanently: RF restored after grace
period; kill and restart within grace: node catches up via the next AE rounds, no transfer storm.

### M8 — GC
(a) Residual contexts: 'g' counter incremented per clean AE round covering an empty-Fields
doc; at 2, delete doc + counter (in-batch). (b) Actor retirement: after a node is dead past
grace AND every owner's VV[actor] is mutually confirmed equal during AE, fold its VV entry
into a retired-actors set and drop it from contexts on next write/compaction touch.
**Accept:** delete 1M docs → after 2 AE rounds 'd' and 'g' prefixes for them are empty
(verified by scan); resurrection test: replica offline during delete, returns after GC →
key stays deleted (this MUST pass — it is the core correctness claim); contexts contain
no retired actors after churn test of 10 node replacements.

### M9 — Hardening
Bounded queues + load shedding everywhere; Prometheus metrics: `delta_lag_seconds{peer}`,
`ae_keys_repaired_total`, `retry_queue_depth{peer}`, `deltas_dropped_total`,
membership generation, `transfer_bytes_total`; pprof endpoints; chaos suite in
test/cluster (random partitions, pauses, clock skew up to ±5s on HLC, crash-restarts)
running invariant checks (section 5) continuously; benchmark suite (target: report
p50/p99 write latency for 1KB docs plus replication convergence latency on a 5-node
in-process cluster; optimize allocations with
sync.Pool on hot paths only after profiles justify it).
**Accept:** 1-hour chaos run with zero invariant violations and zero leaked goroutines;
`-race` clean throughout; benchmark report committed.

---

## 5. Invariants (encode as property tests, run in CI and chaos suite)

1. **Convergence:** any set of deltas applied in any order, with any duplication, to all
   replicas of a partition yields byte-identical documents (Fields and Context).
2. **Merge laws:** commutative, associative, idempotent for arbitrary generated documents.
3. **No resurrection:** if `Context.Contains(r.Dot)` anywhere, register `r` never reappears
   via any merge sequence — including after residual-context GC has run on all owners.
4. **No lost concurrent fields:** concurrent Puts to *different* fields both survive on all
   replicas.
5. **LWW determinism:** concurrent Puts to the *same* field converge to the same single
   winner on all replicas regardless of delivery order.
6. **Dot uniqueness:** no (Actor, Seq) is ever minted twice, across crashes (persisted seq).
7. **Bounded contexts:** steady-state context size per doc ≤ (current owners + retired-set
   slack); never grows with number of operations or deletes.
8. **Placement determinism:** identical membership views → identical owner tables on every
   node.

---

## 6. Explicit deferrals (do not build, do not block on)

- Write/read quorums, read repair, and persistent acked per-peer delta buffers — all
  retrofittable later without changing the wire format or merge semantics; the in-memory
  retry queue is the designated extension point for persistent buffers.
- Hinted handoff / sloppy quorum (writes fail retryably only when no owner is reachable).
- Field-wise merge **inside** nested objects (arbitrary-depth CRDT). Nested objects are
  accepted now but resolved as opaque LWW values; deep merge later becomes a recursive
  register variant — do not flatten this possibility away in the codec or wire format.
- Counters, multi-value registers, sequence CRDTs for arrays.
- Zone/rack-aware HRW weighting.
- Range-based set reconciliation (Merkle is the chosen mechanism).
- Client libraries beyond the gRPC proto; auth/TLS hardening beyond gRPC defaults.
