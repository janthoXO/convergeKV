# convergeKV

**A distributed key-value store that never says no.**

convergeKV is an **AP** (available + partition-tolerant) key-value database. It
keeps accepting reads and writes even when the network splits or machines die,
and it reconciles any divergence automatically — no manual conflict resolution,
no lost updates, no human in the loop. Every node runs the same code and is an
equal peer: there is no leader, no coordinator, and no single point of failure.

It is built for workloads that value **uptime and low write latency** over strict
read-after-write consistency: session stores, user profiles, shopping carts,
feature flags, device/IoT state, and similar "always writable" data.

> Trade-off in one sentence: convergeKV chooses to stay online during failures
> and let replicas briefly disagree, using a conflict-free data model that
> guarantees they converge to the same state once they can talk again.

---

## Table of contents

- [Why convergeKV](#why-convergekv)
- [Feature overview](#feature-overview)
- [Quickstart](#quickstart)
  - [Run a cluster with Docker](#run-a-cluster-with-docker)
  - [Run a single binary](#run-a-single-binary)
- [Talking to the cluster](#talking-to-the-cluster)
- [Configuration](#configuration)
- [Data model](#data-model)
- [How it works (in brief)](#how-it-works-in-brief)
- [Operations](#operations)
- [Performance](#performance)
- [When to use it (and when not to)](#when-to-use-it-and-when-not-to)
- [Project status](#project-status)

---

## Why convergeKV

Most databases force a choice when the network misbehaves: stop serving (to stay
correct) or keep serving (and risk conflicts). convergeKV is unapologetically in
the second camp, but removes the usual downside — conflicts — by storing data as
a **conflict-free replicated data type (CRDT)**. Concurrent writes to the same
key on different nodes don't clobber each other or require a tie-break you have to
think about; the system merges them deterministically and every replica ends up
identical.

What you get in practice:

- **Writes don't wait.** A write is acknowledged as soon as **one** replica has
  durably stored it — no waiting for a quorum or for slow/dead peers.
- **Survives partitions and node loss.** Each key is replicated to 3 nodes; the
  cluster keeps serving as long as one of them is reachable.
- **Self-healing.** A background reconciliation process continuously repairs any
  replica that fell behind. You never run a repair tool by hand.
- **No operator babysitting.** Nodes join and leave by gossip; data placement is
  computed identically by every node with no central registry to configure.

## Feature overview

| Capability | What it means for you |
|---|---|
| **Always available** | Reads and writes continue during network partitions and node failures. |
| **Automatic conflict resolution** | Concurrent updates merge deterministically (per-field last-writer-wins); replicas always converge. |
| **No quorum on the write path** | Low, predictable write latency: one durable local write, then asynchronous replication. |
| **Symmetric, leaderless nodes** | Every node is identical. Add capacity by adding nodes; remove nodes freely. |
| **Automatic data placement** | Keys are sharded across partitions and replicated to 3 owners, recomputed automatically as membership changes. |
| **Self-healing replication** | A periodic Merkle-tree comparison repairs divergence in the background — the single, simple repair mechanism. |
| **Durable storage** | Each write is persisted to an embedded LSM engine (Pebble) before it is acknowledged. |
| **JSON documents** | Values are JSON objects; each top-level field is tracked independently, so concurrent edits to different fields never conflict. |
| **gRPC API** | A small, typed client API: `Get`, `Put` (replace), `Patch` (partial update), `Delete`. |
| **Observability** | Built-in Prometheus metrics and Go pprof endpoints. |
| **Container-ready** | Ships as a tiny static binary and a minimal Docker image. |

## Quickstart

### Run a cluster with Docker

The repository includes a Docker Compose setup that starts a seed node plus
scalable peers.

```bash
# Start a 3-node cluster (1 seed + 2 peers)
make docker-up N=3

# Scale to any size
make docker-up N=5

# Tear it down (and remove volumes)
make docker-down
```

The seed node's client API is published on **`localhost:7000`**.

Optional monitoring stack (Prometheus + Grafana):

```bash
docker compose --profile monitoring up
# Grafana:    http://localhost:3000  (anonymous admin)
# Prometheus: http://localhost:9090
```

### Run a single binary

Requires Go 1.26+.

```bash
make build            # produces ./dist/kvnode

# Node 1 — bootstrap a brand-new cluster (no seeds)
CONVERGEKV_DATA_DIR=./data/n1 \
CONVERGEKV_CLIENT_ADDR=:7000 \
CONVERGEKV_NODE_ADDR=:7001 \
CONVERGEKV_GOSSIP_ADDR=:7946 \
./dist/kvnode

# Node 2 — join via node 1's gossip address (distinct ports on the same host)
CONVERGEKV_DATA_DIR=./data/n2 \
CONVERGEKV_CLIENT_ADDR=:7100 \
CONVERGEKV_NODE_ADDR=:7101 \
CONVERGEKV_GOSSIP_ADDR=:7947 \
CONVERGEKV_ADMIN_ADDR=:7102 \
CONVERGEKV_SEEDS=127.0.0.1:7946 \
./dist/kvnode
```

A node with an empty `SEEDS` starts a new cluster; a node with `SEEDS` joins an
existing one.

## Talking to the cluster

convergeKV exposes a gRPC service `convergekv.KV`:

| Method | Request | Notes |
|---|---|---|
| `Put`    | `{ key, value }` | **Replace.** `value` is the **bytes of a non-empty JSON object**; any field the document currently has that `value` omits is removed. |
| `Patch`  | `{ key, value, delete_fields }` | **Partial update.** Sets the fields in `value` and removes those named in `delete_fields`; fields you don't mention are kept. `value` may be empty when only deleting. |
| `Get`    | `{ key }`        | Returns `{ found, value, context_hash }`. |
| `Delete` | `{ key }`        | Removes the whole key (leaves an internal tombstone, reclaimed automatically). |

> **Replace is add-wins, not authoritative.** `Put` (and `Patch`'s deletes) only
> remove fields the chosen owner has already observed. A field added concurrently
> on another node that this owner hasn't seen yet survives the merge — there is no
> hard whole-document replace under concurrency.

Any node accepts any request; if it isn't an owner of the key, it forwards
internally (at most one extra hop). You can connect to **any** node.

> **Encoding note:** `value` is a protobuf `bytes` field. In native gRPC clients
> (e.g. Go, Node.js) you pass the raw JSON bytes directly. In JSON-based gRPC
> tools (grpcurl, Bruno, Postman), a `bytes` field must be a **base64 string**.

Example with [`grpcurl`](https://github.com/fullstorydev/grpcurl) (the document
`{"name":"Alice"}` base64-encodes to `eyJuYW1lIjoiQWxpY2UifQ==`):

```bash
# Put
grpcurl -plaintext -import-path pkg/proto -proto kv.proto \
  -d '{"key":"user:1","value":"eyJuYW1lIjoiQWxpY2UifQ=="}' \
  127.0.0.1:7000 convergekv.KV/Put

# Get  ->  { "found": true, "value": "eyJuYW1lIjoiQWxpY2UifQ==", ... }
grpcurl -plaintext -import-path pkg/proto -proto kv.proto \
  -d '{"key":"user:1"}' \
  127.0.0.1:7000 convergekv.KV/Get

# Patch: set "tier", remove "name"  ({"tier":"gold"} → eyJ0aWVyIjoiZ29sZCJ9)
grpcurl -plaintext -import-path pkg/proto -proto kv.proto \
  -d '{"key":"user:1","value":"eyJ0aWVyIjoiZ29sZCJ9","delete_fields":["name"]}' \
  127.0.0.1:7000 convergekv.KV/Patch

# Delete
grpcurl -plaintext -import-path pkg/proto -proto kv.proto \
  -d '{"key":"user:1"}' \
  127.0.0.1:7000 convergekv.KV/Delete
```

A ready-made [Bruno](https://www.usebruno.com/) collection lives in
[`docs/bruno/`](docs/bruno/).

## Configuration

convergeKV is configured **entirely through environment variables** (prefix
`CONVERGEKV_`). There are no command-line flags. A bad configuration fails
startup loudly.

| Variable | Default | Description |
|---|---|---|
| `CONVERGEKV_DATA_DIR` | `data` | Directory for the node's identity and on-disk data. |
| `CONVERGEKV_CLIENT_ADDR` | `:7000` | Listen address for the client gRPC API. |
| `CONVERGEKV_NODE_ADDR` | `:7001` | Listen address for node-to-node gRPC. |
| `CONVERGEKV_GOSSIP_ADDR` | `:7946` | Bind address for cluster membership gossip. |
| `CONVERGEKV_ADMIN_ADDR` | `:7002` | Prometheus metrics + pprof. Empty disables it. |
| `CONVERGEKV_ADVERTISE_ADDR` | _(derived)_ | Address other nodes use to reach this one. |
| `CONVERGEKV_SEEDS` | _(empty)_ | Comma-separated gossip addresses to join. Empty = bootstrap a new cluster. |
| `CONVERGEKV_PARTITIONS` | `256` | Cluster-wide shard count. Power of two, ≤ 1024. **Fixed at cluster birth.** |
| `CONVERGEKV_CRASH_GRACE_PERIOD` | `10m` | How long a dead node keeps its data slot before successors take over. |
| `CONVERGEKV_ANTI_ENTROPY_INTERVAL` | `45s` | How often replicas reconcile via Merkle comparison. |
| `CONVERGEKV_REPLICATION_MAX_AGE` | `20s` | Max age a queued replication update may reach before it's dropped to the anti-entropy backstop. Must be < `ANTI_ENTROPY_INTERVAL / 2`. |
| `CONVERGEKV_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. Logs are JSON. |

> `PARTITIONS` is chosen once when the cluster is created and **cannot change**.
> A node that tries to join with a different value is rejected.

## Data model

- A **value is a JSON object**, e.g. `{"name":"Alice","tier":"gold"}`.
- Each **top-level field is tracked independently**, so concurrent edits to
  *different* fields merge without conflict.
- **`Put` replaces** the document (it drops fields the new object omits);
  **`Patch`** sets the fields you provide and deletes the ones you list, leaving
  the rest untouched. Two clients using `Patch` to set different fields on the
  same key both survive the merge.
- If two clients concurrently write the **same field**, the write with the later
  timestamp wins (last-writer-wins), resolved deterministically so every replica
  picks the same winner.
- Field **values are opaque** — strings, numbers, arrays, and nested objects are
  stored verbatim and replaced as a whole (no deep/recursive merge within a
  field).
- Field removal (`Put`'s implicit drops, `Patch`'s `delete_fields`) is
  **add-wins**: only fields the owner has already observed are removed, so a
  concurrent add it hasn't seen survives.
- A `Put` value must be a **non-empty** JSON object (use `Delete` to remove a
  whole key); a `Patch` may omit `value` when it only deletes fields.

## How it works (in brief)

Clients connect to **any** node; nodes are symmetric peers that gossip about
membership and replicate data to each other.

![Cluster overview: clients talk to any node; nodes gossip and replicate](docs/readme/overview.svg)

1. **Placement.** Each key hashes to one of `P` partitions. Each partition is
   owned by 3 nodes, chosen by a shared ranking function (rendezvous hashing)
   that every node computes identically. No central placement service.
2. **Writes.** Your request lands on any node, which routes it to one owner (the
   *applier*). The applier stores the change durably and immediately acknowledges
   you — **without** waiting for the other two owners.
3. **Replication.** The applier then forwards the change to the other owners in
   the background (best-effort, fire-and-forget).
4. **Self-healing.** Periodically, owners of a partition compare compact
   fingerprints (a Merkle tree) and exchange whatever one is missing. This is the
   single mechanism that guarantees every replica eventually converges, and it's
   what makes the best-effort replication safe.
5. **Membership.** Nodes discover each other and detect failures via a gossip
   protocol. Placement is recomputed automatically as nodes come and go.

The write path makes the "no quorum" trade-off concrete — the client is
acknowledged after one durable local write, and replication happens afterward:

![Write path: client acknowledged after one durable write; replication is asynchronous](docs/readme/write-path.svg)

## Operations

- **Scaling out:** start more nodes pointing at existing seeds. Data rebalances
  automatically (new owners bootstrap their partitions from current owners).
- **Scaling in / planned removal:** stop a node gracefully; it hands its data
  responsibilities to successors before leaving, preserving the replication
  factor.
- **Node restart:** a node that restarts quickly resumes its data with no
  re-transfer. A node that was gone longer than `CRASH_GRACE_PERIOD` rejoins as a
  fresh member and re-syncs cleanly.
- **Monitoring:** scrape `CONVERGEKV_ADMIN_ADDR` (`/metrics`). Useful series
  include replication backlog/drops, anti-entropy repairs, data-transfer
  activity, and membership size. Go pprof is served on the same address.

## Performance

Indicative latencies from the in-repo benchmark (5-node cluster, 16 partitions,
RF=3, 1 KB JSON documents, single sequential client; Apple Silicon, synced
writes):

| Operation | p50 | p99 |
|---|---|---|
| Put (durable, single owner) | ~12 ms | ~21 ms |
| Get (local read from an owner) | ~59 µs | ~205 µs |
| Convergence (write byte-equal on all 3 owners) | ~30 ms | ~42 ms |

Put latency is dominated by the per-write `fsync` that makes "acknowledged" mean
"durable." Reads that hit an owner never touch the network. See
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) to reproduce.

## When to use it (and when not to)

**Good fit:**

- High write availability matters more than reading your own write instantly.
- Data naturally tolerates eventual consistency: profiles, sessions, carts,
  preferences, presence, device state, counters-as-registers.
- You want operational simplicity: no leader election, no quorum tuning, no
  manual conflict handling.

**Not a fit:**

- You need strict linearizable / read-after-write consistency or transactions
  across keys.
- You need uniqueness constraints, secondary indexes, or range/SQL queries.
- A field's value requires *merging* concurrent edits rather than
  last-writer-wins (convergeKV treats each field value as an opaque whole).

## Project status

convergeKV is an educational/research implementation of a causal δ-CRDT store. It
is exercised by property, fuzz, chaos, and integration test suites.

For the architecture and contribution guide, see
[`README_DEV.md`](README_DEV.md). A deep, chapter-by-chapter explanation of the
design starts at the [concepts overview](docs/concepts/01-overview.md), which
links to a chapter for every subsystem.
