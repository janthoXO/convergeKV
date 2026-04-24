# ConvergeKV — Agent Implementation Plan

**Language:** Go 1.26+  
**Deployment:** Docker Compose (3-node cluster)  
**Merge logic:** Level 2 field-wise AWLWWMap  
**Storage:** BadgerDB (pure-Go embedded KV; no CGo complexity)  
**API transport:** gRPC  

---

## 0. Ground Rules for the Agent

- Complete each phase fully before starting the next. Every phase ends with a listed verification step.
- Never skip a verification step. If verification fails, fix it before proceeding.
- Never use `interface{}` for JSON values — use `json.RawMessage` to keep field values opaque at the CRDT layer.
- Every exported function must have a Go doc comment.
- `go vet ./...` and `go build ./...` must pass at the end of every phase.

---

## Directory Layout (target state)

```
convergekv/
├── cmd/
│   └── server/
│       └── main.go               # Entry point: wires everything together
├── internal/
│   ├── hlc/
│   │   ├── hlc.go                # HLC type, Send(), Receive(), Compare()
│   │   └── hlc_test.go
│   ├── crdt/
│   │   ├── types.go              # FieldEntry, AWLWWMap type definitions
│   │   ├── merge.go              # Merge(a, b AWLWWMap) AWLWWMap
│   │   └── merge_test.go
│   ├── storage/
│   │   ├── badger.go             # BadgerDB wrapper: Load, Save, Scan
│   │   └── badger_test.go
│   ├── node/
│   │   ├── node.go               # Node: owns HLC, in-memory state, storage
│   │   ├── put.go                # Put() method
│   │   ├── get.go                # Get() method
│   │   └── delete.go             # Delete() method
│   ├── replication/
│   │   ├── antientropy.go        # Anti-entropy loop goroutine
│   │   ├── context.go            # CausalContext: tracks per-peer HLC high watermark
│   │   └── handler.go            # gRPC handler for inbound sync requests
│   └── api/
│       └── handler.go            # gRPC handler for client-facing KV operations
├── proto/
│   ├── kv/
│   │   └── kv.proto              # Client API: Put, Get, Delete, Status
│   └── replication/
│       └── replication.proto     # Sync: SyncRequest, SyncResponse, DeltaEntry
├── gen/                          # Generated protobuf Go code (do not edit)
│   ├── kv/
│   └── replication/
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── go.mod
```

---

## Phase 1 — Project Scaffold

### 1.1 Initialise the Go module

```bash
go mod init github.com/janthoXO/convergeKV
```

### 1.2 Install dependencies

```bash
go get github.com/dgraph-io/badger/v4
go get google.golang.org/grpc
go get google.golang.org/protobuf
go get github.com/google/uuid
```

Install the protobuf code-generation tools (run once on the host, not in Docker):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### 1.3 Makefile

Create `Makefile` with these targets:

```makefile
PROTO_KV        = proto/kv/kv.proto
PROTO_REP       = proto/replication/replication.proto

.PHONY: proto build test docker-up docker-down

proto:
	protoc --go_out=gen --go-grpc_out=gen \
	       --go_opt=paths=source_relative \
	       --go-grpc_opt=paths=source_relative \
	       $(PROTO_KV) $(PROTO_REP)

build:
	go build ./...

test:
	go test ./... -race -count=1

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
```

### 1.4 Write the proto files

**`proto/kv/kv.proto`**

```proto
syntax = "proto3";
package kv;
option go_package = "github.com/janthoXO/convergeKV/gen/kv";

// HLCTimestamp captures a Hybrid Logical Clock value.
message HLCTimestamp {
  uint64 physical_ms = 1;  // wall-clock milliseconds
  uint32 logical     = 2;  // logical counter
}

// PutRequest carries a key and a flat JSON object as a UTF-8 string.
// Example value: {"name":"Alice","age":30}
message PutRequest {
  string key        = 1;
  string value_json = 2;   // must be a JSON object (not array/scalar)
}
message PutResponse {
  HLCTimestamp timestamp = 1;  // HLC assigned to this write
}

message GetRequest  { string key = 1; }
message GetResponse {
  string       value_json = 1;  // merged JSON object; empty string if key absent
  bool         found      = 2;
}

message DeleteRequest  { string key = 1; }
message DeleteResponse { HLCTimestamp timestamp = 1; }

message StatusRequest {}
message StatusResponse {
  string       replica_id = 1;
  HLCTimestamp hlc        = 2;
  repeated string peers   = 3;
}

service KVService {
  rpc Put    (PutRequest)    returns (PutResponse);
  rpc Get    (GetRequest)    returns (GetResponse);
  rpc Delete (DeleteRequest) returns (DeleteResponse);
  rpc Status (StatusRequest) returns (StatusResponse);
}
```

**`proto/replication/replication.proto`**

```proto
syntax = "proto3";
package replication;
option go_package = "github.com/janthoXO/convergeKV/gen/replication";

import "kv/kv.proto";

// CausalContext is a compact summary of what a replica has already seen.
// Keys are replica_ids; values are the highest HLC timestamp seen FROM that replica.
message CausalContext {
  map<string, kv.HLCTimestamp> seen = 1;
}

// DeltaEntry is the unit of replication: one field of one key.
message DeltaEntry {
  string          key        = 1;
  string          field      = 2;
  bytes           value_json = 3;  // raw JSON bytes for this field's value
  kv.HLCTimestamp timestamp  = 4;
  string          replica_id = 5;
  bool            deleted    = 6;  // true = tombstone
}

message SyncRequest {
  string        replica_id = 1;
  CausalContext context    = 2;
}

message SyncResponse {
  repeated DeltaEntry deltas  = 1;
  CausalContext       context = 2;  // sender's context so receiver can update its view
}

service ReplicationService {
  // Sync is called by a peer during anti-entropy. The caller sends its
  // causal context; the responder replies with all deltas the caller hasn't seen.
  rpc Sync (SyncRequest) returns (SyncResponse);
}
```

### 1.5 Generate Go code

```bash
make proto
```

Verify that `gen/kv/` and `gen/replication/` contain `*.pb.go` and `*_grpc.pb.go` files.

### ✅ Phase 1 verification

```bash
go build ./...   # must produce no errors
```

---

## Phase 2 — Hybrid Logical Clock

### File: `internal/hlc/hlc.go`

Implement the `HLC` struct and three operations. **No external libraries** — implement from scratch.

```go
package hlc

import (
    "sync"
    "time"
)

// Timestamp is a Hybrid Logical Clock value.
// It is totally ordered: compare PhysicalMs first, then Logical.
type Timestamp struct {
    PhysicalMs uint64
    Logical    uint32
}

// HLC is a thread-safe Hybrid Logical Clock.
type HLC struct {
    mu      sync.Mutex
    current Timestamp
}

// New returns an HLC initialised to the current wall time.
func New() *HLC { return &HLC{} }

// Send is called before originating a local event (a write).
// It advances the clock and returns the new timestamp.
func (h *HLC) Send() Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    pt := wallMs()
    if pt > h.current.PhysicalMs {
        h.current = Timestamp{PhysicalMs: pt, Logical: 0}
    } else {
        h.current.Logical++
    }
    return h.current
}

// Receive is called when a message carrying remote timestamp r arrives.
// It advances the local clock to be strictly greater than both the local
// state and the remote timestamp, then returns the new timestamp.
func (h *HLC) Receive(r Timestamp) Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    pt := wallMs()
    maxPhys := max3(pt, h.current.PhysicalMs, r.PhysicalMs)
    switch {
    case maxPhys > h.current.PhysicalMs && maxPhys > r.PhysicalMs:
        h.current = Timestamp{PhysicalMs: maxPhys, Logical: 0}
    case maxPhys == h.current.PhysicalMs && maxPhys > r.PhysicalMs:
        h.current.Logical++
    case maxPhys == r.PhysicalMs && maxPhys > h.current.PhysicalMs:
        h.current = Timestamp{PhysicalMs: maxPhys, Logical: r.Logical + 1}
    default: // maxPhys == both physical parts
        if h.current.Logical >= r.Logical {
            h.current.Logical++
        } else {
            h.current = Timestamp{PhysicalMs: maxPhys, Logical: r.Logical + 1}
        }
    }
    return h.current
}

// Now returns the current clock value without advancing it.
func (h *HLC) Now() Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    return h.current
}

// Less returns true if a is strictly less than b.
func Less(a, b Timestamp) bool {
    if a.PhysicalMs != b.PhysicalMs {
        return a.PhysicalMs < b.PhysicalMs
    }
    return a.Logical < b.Logical
}

// Equal returns true if a and b represent the same instant.
func Equal(a, b Timestamp) bool {
    return a.PhysicalMs == b.PhysicalMs && a.Logical == b.Logical
}

func wallMs() uint64 { return uint64(time.Now().UnixMilli()) }

func max3(a, b, c uint64) uint64 {
    m := a
    if b > m { m = b }
    if c > m { m = c }
    return m
}
```

### File: `internal/hlc/hlc_test.go`

Write tests for:
- `Send()` always returns a timestamp strictly greater than the previous `Send()` result.
- `Receive()` with a remote timestamp greater than local produces a result greater than both.
- `Less()` / `Equal()` are consistent.
- Concurrent calls to `Send()` from 100 goroutines all produce distinct timestamps (race-free).

### ✅ Phase 2 verification

```bash
go test ./internal/hlc/... -race -count=3
```

---

## Phase 3 — CRDT Types and Field-Wise Merge

### File: `internal/crdt/types.go`

```go
package crdt

import (
    "encoding/json"
    "github.com/janthoXO/convergeKV/internal/hlc"
)

// FieldEntry is the per-field CRDT value stored for each (key, field) pair.
type FieldEntry struct {
    // Value holds the raw JSON bytes of this field's value (e.g. `"Alice"`, `30`, `true`).
    // It is nil for a tombstone.
    Value     json.RawMessage
    Timestamp hlc.Timestamp
    ReplicaID string
    Deleted   bool // true = this entry is a tombstone (field was deleted)
}

// AWLWWMap is the per-key CRDT state: a map of field name -> FieldEntry.
// The zero value is a valid empty map.
type AWLWWMap struct {
    Fields map[string]FieldEntry // never nil after initialisation
}

// NewAWLWWMap returns an empty, ready-to-use AWLWWMap.
func NewAWLWWMap() AWLWWMap {
    return AWLWWMap{Fields: make(map[string]FieldEntry)}
}

// WinsOver returns true if candidate should replace existing under the LWW rule:
//   compare (Timestamp, ReplicaID) lexicographically; ReplicaID breaks ties.
func WinsOver(candidate, existing FieldEntry) bool {
    if hlc.Less(existing.Timestamp, candidate.Timestamp) {
        return true
    }
    if hlc.Equal(existing.Timestamp, candidate.Timestamp) {
        return candidate.ReplicaID > existing.ReplicaID
    }
    return false
}
```

### File: `internal/crdt/merge.go`

```go
package crdt

// Merge computes the join of two AWLWWMap values.
//
// Properties guaranteed by this implementation:
//   - Commutative:  Merge(a, b) == Merge(b, a)
//   - Associative:  Merge(Merge(a, b), c) == Merge(a, Merge(b, c))
//   - Idempotent:   Merge(a, a) == a
//
// For each field, the entry with the strictly greater (Timestamp, ReplicaID)
// tuple wins. Both non-overlapping fields are preserved.
// Tombstones compete under the same rule: a tombstone with a higher timestamp
// beats a live entry, and vice versa.
func Merge(a, b AWLWWMap) AWLWWMap {
    result := NewAWLWWMap()

    // Start with a copy of a.
    for field, entry := range a.Fields {
        result.Fields[field] = entry
    }

    // Merge in every field from b.
    for field, bEntry := range b.Fields {
        aEntry, exists := result.Fields[field]
        if !exists || WinsOver(bEntry, aEntry) {
            result.Fields[field] = bEntry
        }
    }
    return result
}

// Apply merges a single FieldEntry into an existing AWLWWMap in-place.
// Use this on the hot write path instead of a full Merge() call.
func Apply(m *AWLWWMap, field string, incoming FieldEntry) {
    if m.Fields == nil {
        m.Fields = make(map[string]FieldEntry)
    }
    existing, exists := m.Fields[field]
    if !exists || WinsOver(incoming, existing) {
        m.Fields[field] = incoming
    }
}

// ToJSON reconstructs the JSON object represented by m.
// Deleted (tombstone) fields are omitted.
// Returns nil if all fields are tombstones or the map is empty.
func ToJSON(m AWLWWMap) ([]byte, bool) {
    obj := make(map[string]json.RawMessage)
    for field, entry := range m.Fields {
        if !entry.Deleted {
            obj[field] = entry.Value
        }
    }
    if len(obj) == 0 {
        return nil, false
    }
    b, _ := json.Marshal(obj)
    return b, true
}
```

### File: `internal/crdt/merge_test.go`

Write table-driven tests covering **every row of the acceptance criteria merge table**:

| Test case | Setup | Expected |
|---|---|---|
| Field only in A | `a={name:Alice}`, `b={}` | result has `name:Alice` |
| Field only in B | `a={}`, `b={age:30}` | result has `age:30` |
| Non-overlapping fields | `a={name:Alice}`, `b={age:30}` | result has both |
| Same field, A wins on timestamp | `a={v:1, t=10}`, `b={v:2, t=5}` | result `v=1` |
| Same field, B wins on timestamp | `a={v:1, t=5}`, `b={v:2, t=10}` | result `v=2` |
| Same field, same timestamp, A wins replica_id | `a={v:1, rid="r2"}`, `b={v:2, rid="r1"}` | result `v=1` |
| Tombstone beats live entry (higher ts) | `a={v:1,t=5}`, `b={deleted,t=10}` | result is tombstone |
| Live entry beats tombstone (higher ts) | `a={v:1,t=10}`, `b={deleted,t=5}` | result `v=1` |
| Commutativity | random pairs | `Merge(a,b)==Merge(b,a)` |
| Idempotence | random maps | `Merge(a,a)==a` |
| Associativity | random triples | `Merge(Merge(a,b),c)==Merge(a,Merge(b,c))` |

Equality comparison for `AWLWWMap` in tests: compare `Fields` map key-by-key.

### ✅ Phase 3 verification

```bash
go test ./internal/crdt/... -race -count=1 -v
```

All 11 test cases must pass.

---

## Phase 4 — Storage Layer

### File: `internal/storage/badger.go`

BadgerDB stores each `(key, field)` pair as a single record. The storage key format is:

```
{key}\x00{field}
```

The value is a JSON-serialised `StoredEntry` struct.

```go
package storage

import (
    "encoding/json"
    "strings"

    badger "github.com/dgraph-io/badger/v4"
    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
)

// StoredEntry is the on-disk representation of a single FieldEntry.
type StoredEntry struct {
    ValueJSON  []byte    `json:"value"`      // raw JSON bytes
    PhysicalMs uint64    `json:"phys_ms"`
    Logical    uint32    `json:"logical"`
    ReplicaID  string    `json:"replica_id"`
    Deleted    bool      `json:"deleted"`
}

// Store wraps a BadgerDB instance with CRDT-aware read/write helpers.
type Store struct {
    db *badger.DB
}

// Open opens (or creates) a BadgerDB database at dir.
func Open(dir string) (*Store, error) {
    opts := badger.DefaultOptions(dir).WithLogger(nil)
    db, err := badger.Open(opts)
    if err != nil {
        return nil, err
    }
    return &Store{db: db}, nil
}

// Close releases the BadgerDB handle.
func (s *Store) Close() error { return s.db.Close() }

// badgerKey encodes a (key, field) pair into a storage key.
func badgerKey(key, field string) []byte {
    return []byte(key + "\x00" + field)
}

// decodeKey splits a storage key back into (key, field).
func decodeKey(b []byte) (key, field string) {
    parts := strings.SplitN(string(b), "\x00", 2)
    if len(parts) != 2 {
        return string(b), ""
    }
    return parts[0], parts[1]
}

// SaveField persists a single FieldEntry.
func (s *Store) SaveField(key, field string, entry crdt.FieldEntry) error {
    se := StoredEntry{
        ValueJSON:  entry.Value,
        PhysicalMs: entry.Timestamp.PhysicalMs,
        Logical:    entry.Timestamp.Logical,
        ReplicaID:  entry.ReplicaID,
        Deleted:    entry.Deleted,
    }
    b, err := json.Marshal(se)
    if err != nil {
        return err
    }
    return s.db.Update(func(txn *badger.Txn) error {
        return txn.Set(badgerKey(key, field), b)
    })
}

// LoadAll reads every (key, field) record from BadgerDB and returns the full
// in-memory state as map[key]AWLWWMap. Called once at node startup.
func (s *Store) LoadAll() (map[string]crdt.AWLWWMap, error) {
    result := make(map[string]crdt.AWLWWMap)
    err := s.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
            k := item.KeyCopy(nil)
            v, err := item.ValueCopy(nil)
            if err != nil {
                return err
            }
            var se StoredEntry
            if err := json.Unmarshal(v, &se); err != nil {
                return err
            }
            mapKey, field := decodeKey(k)
            m, ok := result[mapKey]
            if !ok {
                m = crdt.NewAWLWWMap()
            }
            m.Fields[field] = crdt.FieldEntry{
                Value:     se.ValueJSON,
                Timestamp: hlc.Timestamp{PhysicalMs: se.PhysicalMs, Logical: se.Logical},
                ReplicaID: se.ReplicaID,
                Deleted:   se.Deleted,
            }
            result[mapKey] = m
        }
        return nil
    })
    return result, err
}

// SaveBatch persists a batch of field entries atomically (used during replication).
// Accepts a slice of (key, field, FieldEntry) tuples.
func (s *Store) SaveBatch(entries []FieldUpdate) error {
    wb := s.db.NewWriteBatch()
    defer wb.Cancel()
    for _, u := range entries {
        se := StoredEntry{
            ValueJSON:  u.Entry.Value,
            PhysicalMs: u.Entry.Timestamp.PhysicalMs,
            Logical:    u.Entry.Timestamp.Logical,
            ReplicaID:  u.Entry.ReplicaID,
            Deleted:    u.Entry.Deleted,
        }
        b, err := json.Marshal(se)
        if err != nil {
            return err
        }
        if err := wb.Set(badgerKey(u.Key, u.Field), b); err != nil {
            return err
        }
    }
    return wb.Flush()
}

// FieldUpdate is a helper tuple used in SaveBatch.
type FieldUpdate struct {
    Key   string
    Field string
    Entry crdt.FieldEntry
}
```

### ✅ Phase 4 verification

Write `internal/storage/badger_test.go` that:
1. Opens a BadgerDB in a `t.TempDir()`.
2. Saves five field entries across two keys.
3. Calls `LoadAll()` and asserts all entries are recovered with correct field values, timestamps, and replica IDs.

```bash
go test ./internal/storage/... -race -count=1
```

---

## Phase 5 — Node (Business Logic)

The `node.Node` owns the HLC, the in-memory state map, and the storage layer. All public methods are thread-safe.

### File: `internal/node/node.go`

```go
package node

import (
    "sync"

    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/storage"
)

// Node is the central state holder. It is safe for concurrent use.
type Node struct {
    mu        sync.RWMutex
    replicaID string
    hlc       *hlc.HLC
    state     map[string]crdt.AWLWWMap // key -> per-field CRDT map
    store     *storage.Store
}

// New constructs a Node, loading existing state from the store.
func New(replicaID string, store *storage.Store) (*Node, error) {
    existing, err := store.LoadAll()
    if err != nil {
        return nil, err
    }
    return &Node{
        replicaID: replicaID,
        hlc:       hlc.New(),
        state:     existing,
        store:     store,
    }, nil
}

// ReplicaID returns the node's stable identifier.
func (n *Node) ReplicaID() string { return n.replicaID }

// HLCNow returns the node's current HLC value.
func (n *Node) HLCNow() hlc.Timestamp { return n.hlc.Now() }

// ReceiveHLC advances the node's HLC with a remote timestamp.
func (n *Node) ReceiveHLC(remote hlc.Timestamp) hlc.Timestamp {
    return n.hlc.Receive(remote)
}

// getMap returns (a copy of) the AWLWWMap for key, or an empty map.
// Caller must not hold n.mu.
func (n *Node) getMap(key string) crdt.AWLWWMap {
    n.mu.RLock()
    defer n.mu.RUnlock()
    m, ok := n.state[key]
    if !ok {
        return crdt.NewAWLWWMap()
    }
    // shallow copy: Fields map reference is safe because we only read it
    return m
}
```

### File: `internal/node/put.go`

```go
package node

import (
    "encoding/json"
    "fmt"

    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/storage"
)

// Put writes a JSON object value to key. The value must be a JSON object
// (i.e., start with '{'}). Each field is stored independently.
// Returns the HLC timestamp assigned to this write.
func (n *Node) Put(key, valueJSON string) (hlc.Timestamp, error) {
    var obj map[string]json.RawMessage
    if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
        return hlc.Timestamp{}, fmt.Errorf("put: value must be a JSON object: %w", err)
    }

    ts := n.hlc.Send()

    n.mu.Lock()
    defer n.mu.Unlock()

    m, ok := n.state[key]
    if !ok {
        m = crdt.NewAWLWWMap()
    }

    var batch []storage.FieldUpdate
    for field, raw := range obj {
        entry := crdt.FieldEntry{
            Value:     raw,
            Timestamp: ts,
            ReplicaID: n.replicaID,
            Deleted:   false,
        }
        crdt.Apply(&m, field, entry)
        batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: entry})
    }

    n.state[key] = m
    if err := n.store.SaveBatch(batch); err != nil {
        return hlc.Timestamp{}, fmt.Errorf("put: storage error: %w", err)
    }
    return ts, nil
}
```

### File: `internal/node/get.go`

```go
package node

import "github.com/janthoXO/convergeKV/internal/crdt"

// Get returns the current JSON representation of key.
// Returns ("", false) if the key does not exist or all fields are tombstones.
func (n *Node) Get(key string) (string, bool) {
    m := n.getMap(key)
    b, ok := crdt.ToJSON(m)
    if !ok {
        return "", false
    }
    return string(b), true
}
```

### File: `internal/node/delete.go`

```go
package node

import (
    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/storage"
    "fmt"
)

// Delete marks all current fields of key as tombstones.
// Fields added after the delete timestamp are NOT affected (they will win on merge).
func (n *Node) Delete(key string) (hlc.Timestamp, error) {
    ts := n.hlc.Send()

    n.mu.Lock()
    defer n.mu.Unlock()

    m, ok := n.state[key]
    if !ok {
        return ts, nil // nothing to delete
    }

    var batch []storage.FieldUpdate
    for field := range m.Fields {
        tombstone := crdt.FieldEntry{
            Value:     nil,
            Timestamp: ts,
            ReplicaID: n.replicaID,
            Deleted:   true,
        }
        m.Fields[field] = tombstone
        batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: tombstone})
    }
    n.state[key] = m

    if err := n.store.SaveBatch(batch); err != nil {
        return hlc.Timestamp{}, fmt.Errorf("delete: storage error: %w", err)
    }
    return ts, nil
}
```

### File: `internal/node/merge_incoming.go`

This is called by the replication layer when a delta arrives from a peer.

```go
package node

import (
    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/storage"
)

// ApplyDelta merges a single incoming field entry from a peer into local state.
// It advances the HLC with the remote timestamp.
// Returns true if the incoming entry actually changed local state.
func (n *Node) ApplyDelta(key, field string, incoming crdt.FieldEntry) (bool, error) {
    _ = n.hlc.Receive(incoming.Timestamp) // advance HLC

    n.mu.Lock()
    defer n.mu.Unlock()

    m, ok := n.state[key]
    if !ok {
        m = crdt.NewAWLWWMap()
    }

    existing, exists := m.Fields[field]
    if exists && !crdt.WinsOver(incoming, existing) {
        return false, nil // local entry already wins; no change
    }

    crdt.Apply(&m, field, incoming)
    n.state[key] = m

    err := n.store.SaveBatch([]storage.FieldUpdate{
        {Key: key, Field: field, Entry: incoming},
    })
    return true, err
}

// Snapshot returns a flat list of every (key, field, FieldEntry) the node holds.
// Used by the anti-entropy sender to compute deltas for a peer.
func (n *Node) Snapshot() []DeltaRecord {
    n.mu.RLock()
    defer n.mu.RUnlock()
    var out []DeltaRecord
    for key, m := range n.state {
        for field, entry := range m.Fields {
            out = append(out, DeltaRecord{Key: key, Field: field, Entry: entry})
        }
    }
    return out
}

// DeltaRecord is a flat (key, field, entry) triple used in replication.
type DeltaRecord struct {
    Key   string
    Field string
    Entry crdt.FieldEntry
}
```

### ✅ Phase 5 verification

Write `internal/node/node_test.go` covering:
- `Put` then `Get` on a single node returns correct merged JSON.
- `Put` two overlapping JSON objects; `Get` returns all fields.
- `Delete` then `Get` returns `found=false`.
- `ApplyDelta` with a higher-timestamp entry overwrites local.
- `ApplyDelta` with a lower-timestamp entry is a no-op.

```bash
go test ./internal/node/... -race -count=1
```

---

## Phase 6 — Replication

### File: `internal/replication/context.go`

```go
package replication

import (
    "sync"
    "github.com/janthoXO/convergeKV/internal/hlc"
)

// CausalContext tracks the highest HLC timestamp seen FROM each known replica.
// It is used to compute which delta entries a peer still needs.
type CausalContext struct {
    mu   sync.RWMutex
    seen map[string]hlc.Timestamp // replicaID -> max timestamp seen from that replica
}

// NewCausalContext returns an empty CausalContext.
func NewCausalContext() *CausalContext {
    return &CausalContext{seen: make(map[string]hlc.Timestamp)}
}

// Update advances the recorded high-watermark for replicaID if ts is greater.
func (c *CausalContext) Update(replicaID string, ts hlc.Timestamp) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if cur, ok := c.seen[replicaID]; !ok || hlc.Less(cur, ts) {
        c.seen[replicaID] = ts
    }
}

// Snapshot returns a copy of the current seen map.
func (c *CausalContext) Snapshot() map[string]hlc.Timestamp {
    c.mu.RLock()
    defer c.mu.RUnlock()
    out := make(map[string]hlc.Timestamp, len(c.seen))
    for k, v := range c.seen {
        out[k] = v
    }
    return out
}

// NeedsEntry returns true if the given (replicaID, ts) entry is NOT yet
// recorded in this context, meaning the holder of this context hasn't seen it.
func (c *CausalContext) NeedsEntry(replicaID string, ts hlc.Timestamp) bool {
    c.mu.RLock()
    defer c.mu.RUnlock()
    cur, ok := c.seen[replicaID]
    if !ok {
        return true
    }
    return hlc.Less(cur, ts)
}
```

### File: `internal/replication/handler.go`

This implements the `ReplicationService` gRPC server that peers call into.

```go
package replication

import (
    "context"

    repb "github.com/janthoXO/convergeKV/gen/replication"
    kvpb "github.com/janthoXO/convergeKV/gen/kv"
    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/node"
)

// Handler implements repb.ReplicationServiceServer.
type Handler struct {
    repb.UnimplementedReplicationServiceServer
    node    *node.Node
    context *CausalContext
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node, ctx *CausalContext) *Handler {
    return &Handler{node: n, context: ctx}
}

// Sync is called by a peer during anti-entropy.
// It returns all delta entries that the caller's causal context indicates it hasn't seen.
func (h *Handler) Sync(_ context.Context, req *repb.SyncRequest) (*repb.SyncResponse, error) {
    // Build the peer's seen map from the request.
    peerSeen := make(map[string]hlc.Timestamp)
    for rid, pts := range req.Context.GetSeen() {
        peerSeen[rid] = hlc.Timestamp{
            PhysicalMs: pts.GetPhysicalMs(),
            Logical:    pts.GetLogical(),
        }
    }

    // Collect all local entries the peer hasn't seen yet.
    snapshot := h.node.Snapshot()
    var deltas []*repb.DeltaEntry
    for _, rec := range snapshot {
        peerHWM, ok := peerSeen[rec.Entry.ReplicaID]
        if !ok || hlc.Less(peerHWM, rec.Entry.Timestamp) {
            deltas = append(deltas, encodeEntry(rec))
        }
    }

    // Return our own causal context so the peer can update its view.
    localCtx := h.context.Snapshot()
    seenPb := make(map[string]*kvpb.HLCTimestamp, len(localCtx))
    for rid, ts := range localCtx {
        seenPb[rid] = &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical}
    }

    return &repb.SyncResponse{
        Deltas:  deltas,
        Context: &repb.CausalContext{Seen: seenPb},
    }, nil
}

func encodeEntry(rec node.DeltaRecord) *repb.DeltaEntry {
    return &repb.DeltaEntry{
        Key:       rec.Key,
        Field:     rec.Field,
        ValueJson: rec.Entry.Value,
        Timestamp: &kvpb.HLCTimestamp{
            PhysicalMs: rec.Entry.Timestamp.PhysicalMs,
            Logical:    rec.Entry.Timestamp.Logical,
        },
        ReplicaId: rec.Entry.ReplicaID,
        Deleted:   rec.Entry.Deleted,
    }
}
```

### File: `internal/replication/antientropy.go`

```go
package replication

import (
    "context"
    "log"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    repb "github.com/janthoXO/convergeKV/gen/replication"
    kvpb  "github.com/janthoXO/convergeKV/gen/kv"
    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/node"
)

// AntiEntropy runs a background goroutine that periodically contacts each peer,
// sends the local causal context, and merges the returned deltas.
type AntiEntropy struct {
    node     *node.Node
    peers    []string // host:port addresses
    context  *CausalContext
    interval time.Duration
}

// NewAntiEntropy returns an AntiEntropy runner.
func NewAntiEntropy(n *node.Node, peers []string, ctx *CausalContext, interval time.Duration) *AntiEntropy {
    return &AntiEntropy{node: n, peers: peers, context: ctx, interval: interval}
}

// Run starts the anti-entropy loop. Call in a goroutine. Stops when ctx is cancelled.
func (ae *AntiEntropy) Run(ctx context.Context) {
    ticker := time.NewTicker(ae.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            for _, peer := range ae.peers {
                ae.syncWithPeer(ctx, peer)
            }
        }
    }
}

func (ae *AntiEntropy) syncWithPeer(ctx context.Context, addr string) {
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Printf("[antientropy] dial %s: %v", addr, err)
        return
    }
    defer conn.Close()

    client := repb.NewReplicationServiceClient(conn)

    // Build the SyncRequest with our current causal context.
    localCtx := ae.context.Snapshot()
    seenPb := make(map[string]*kvpb.HLCTimestamp, len(localCtx))
    for rid, ts := range localCtx {
        seenPb[rid] = &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical}
    }

    resp, err := client.Sync(ctx, &repb.SyncRequest{
        ReplicaId: ae.node.ReplicaID(),
        Context:   &repb.CausalContext{Seen: seenPb},
    })
    if err != nil {
        log.Printf("[antientropy] sync %s: %v", addr, err)
        return
    }

    // Apply each received delta.
    for _, d := range resp.GetDeltas() {
        ts := hlc.Timestamp{
            PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
            Logical:    d.GetTimestamp().GetLogical(),
        }
        entry := crdt.FieldEntry{
            Value:     d.GetValueJson(),
            Timestamp: ts,
            ReplicaID: d.GetReplicaId(),
            Deleted:   d.GetDeleted(),
        }
        if _, err := ae.node.ApplyDelta(d.GetKey(), d.GetField(), entry); err != nil {
            log.Printf("[antientropy] apply delta: %v", err)
        }
        // Update our causal context with the newly seen entry.
        ae.context.Update(d.GetReplicaId(), ts)
    }

    // Also update our context from the peer's returned context.
    for rid, pts := range resp.GetContext().GetSeen() {
        ae.context.Update(rid, hlc.Timestamp{
            PhysicalMs: pts.GetPhysicalMs(),
            Logical:    pts.GetLogical(),
        })
    }
}
```

### ✅ Phase 6 verification

No automated test yet (integration tests cover this in Phase 8). Verify compilation:

```bash
go build ./...
```

---

## Phase 7 — Client API and Entry Point

### File: `internal/api/handler.go`

```go
package api

import (
    "context"

    kvpb "github.com/janthoXO/convergeKV/gen/kv"
    "github.com/janthoXO/convergeKV/internal/node"
    "github.com/janthoXO/convergeKV/internal/replication"
)

// Handler implements kvpb.KVServiceServer.
type Handler struct {
    kvpb.UnimplementedKVServiceServer
    node    *node.Node
    peers   []string
    causal  *replication.CausalContext
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(n *node.Node, peers []string, causal *replication.CausalContext) *Handler {
    return &Handler{node: n, peers: peers, causal: causal}
}

func (h *Handler) Put(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
    ts, err := h.node.Put(req.GetKey(), req.GetValueJson())
    if err != nil {
        return nil, err
    }
    // Record own write in causal context.
    h.causal.Update(h.node.ReplicaID(), ts)
    return &kvpb.PutResponse{
        Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
    }, nil
}

func (h *Handler) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
    v, found := h.node.Get(req.GetKey())
    return &kvpb.GetResponse{ValueJson: v, Found: found}, nil
}

func (h *Handler) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
    ts, err := h.node.Delete(req.GetKey())
    if err != nil {
        return nil, err
    }
    return &kvpb.DeleteResponse{
        Timestamp: &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
    }, nil
}

func (h *Handler) Status(_ context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
    ts := h.node.HLCNow()
    return &kvpb.StatusResponse{
        ReplicaId: h.node.ReplicaID(),
        Hlc:       &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
        Peers:     h.peers,
    }, nil
}
```

### File: `cmd/server/main.go`

```go
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
```

### ✅ Phase 7 verification

```bash
go build ./cmd/server/...   # must produce a binary without errors
go vet ./...                 # must produce no warnings
```

---

## Phase 8 — Docker and Docker Compose

### `Dockerfile`

```dockerfile
# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /convergekv ./cmd/server

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /convergekv /convergekv
ENTRYPOINT ["/convergekv"]
```

Note: `CGO_ENABLED=0` is required because BadgerDB is pure Go and we want a static binary.

### `docker-compose.yml`

```yaml
version: "3.9"

x-node-defaults: &node-defaults
  build: .
  restart: unless-stopped
  networks:
    - convergekv

networks:
  convergekv:
    driver: bridge

services:
  replica1:
    <<: *node-defaults
    container_name: replica1
    environment:
      REPLICA_ID: "replica1"
      PEERS:      "replica2:50051,replica3:50051"
      GRPC_PORT:  "50051"
      DATA_DIR:   "/data"
    volumes:
      - replica1-data:/data
    ports:
      - "50051:50051"   # expose to host for manual testing

  replica2:
    <<: *node-defaults
    container_name: replica2
    environment:
      REPLICA_ID: "replica2"
      PEERS:      "replica1:50051,replica3:50051"
      GRPC_PORT:  "50051"
      DATA_DIR:   "/data"
    volumes:
      - replica2-data:/data
    ports:
      - "50052:50051"

  replica3:
    <<: *node-defaults
    container_name: replica3
    environment:
      REPLICA_ID: "replica3"
      PEERS:      "replica1:50051,replica2:50051"
      GRPC_PORT:  "50051"
      DATA_DIR:   "/data"
    volumes:
      - replica3-data:/data
    ports:
      - "50053:50051"

volumes:
  replica1-data:
  replica2-data:
  replica3-data:
```

### ✅ Phase 8 verification

```bash
make docker-up
# Wait ~5 seconds for all nodes to start
docker compose logs   # must show all 3 nodes listening with their peer lists
```

Use `grpcurl` to smoke-test from the host:

```bash
# Install: brew install grpcurl  OR  go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Put on replica 1
grpcurl -plaintext -d '{"key":"user:1","value_json":"{\"name\":\"Alice\"}"}' \
  localhost:50051 kv.KVService/Put

# Put on replica 2 (while both are running)
grpcurl -plaintext -d '{"key":"user:1","value_json":"{\"age\":30}"}' \
  localhost:50052 kv.KVService/Put

# Wait 3 seconds for anti-entropy, then read from replica 3
sleep 3
grpcurl -plaintext -d '{"key":"user:1"}' \
  localhost:50053 kv.KVService/Get
# Expected: {"value_json":"{\"name\":\"Alice\",\"age\":30}","found":true}
```

---

## Phase 9 — Partition Simulation Utility

Create `cmd/partitiontest/main.go` — a standalone Go program (not a test file) that:

1. Dials all three replicas by address.
2. Calls `docker network disconnect convergekv replica2` via `os/exec` to simulate a partition.
3. Issues `Put("x", {"v": 1})` on replica1 and `Put("x", {"v": 2})` on replica2.
4. Reconnects with `docker network connect convergekv replica2`.
5. Sleeps 5 seconds.
6. Calls `Get("x")` on all three replicas.
7. Prints the HLC timestamps from both writes and explains which `v` won and why.
8. Asserts all three replicas return the same value for `"x"`.

This program serves as the canonical demo described in the acceptance criteria Section 6.

```bash
go run ./cmd/partitiontest --replica1=localhost:50051 \
                            --replica2=localhost:50052 \
                            --replica3=localhost:50053
```

---

## Summary of Build Order

```
Phase 1  →  proto files + go.mod + Makefile
Phase 2  →  internal/hlc         (no dependencies except stdlib)
Phase 3  →  internal/crdt        (depends on hlc)
Phase 4  →  internal/storage     (depends on crdt, badger)
Phase 5  →  internal/node        (depends on crdt, storage, hlc)
Phase 6  →  internal/replication (depends on node, gen/*)
Phase 7  →  internal/api + cmd/server (depends on everything)
Phase 8  →  Dockerfile + docker-compose.yml
Phase 9  →  cmd/partitiontest
```

Each phase's output is a buildable, testable increment. The agent must not proceed to phase N+1 until `go build ./...` and the phase's own test command pass without errors.
