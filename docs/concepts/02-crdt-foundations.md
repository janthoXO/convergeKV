# 2. CRDT Foundations

> **This is the most important chapter.** Everything else in convergeKV exists to
> serve the data model described here. It builds from intuition up to the actual
> code in `internal/crdt`.

## 2.1 The problem: concurrent edits with no boss

Two clients edit the same key at the same time, on two different nodes that
briefly can't talk to each other:

- On Node A: `Put("user:42", {"name": "Alice"})`
- On Node B: `Put("user:42", {"email": "a@x.com"})`

Both succeed (this is an available system — it doesn't block). Now the two nodes
hold *different* state for the same key. When they reconnect, what should happen?

The naive instinct is "use timestamps, newest wins." But these edits touch
*different fields*, so picking one and discarding the other **loses an update** —
the user would mysteriously lose either their name or their email. The right
answer is obviously `{"name": "Alice", "email": "a@x.com"}`: keep both.

Now a harder case — both write the *same* field:

- On Node A: `Put("user:42", {"name": "Alice"})`
- On Node B: `Put("user:42", {"name": "Alicia"})`

Here one value genuinely must be picked (there can't be two names). But whatever
rule is chosen, it must give the **same answer on every node**, no matter what
order the updates arrived in. If Node A decides "Alice" and Node B decides
"Alicia," they never converge and the system is broken.

And the hardest case — delete versus write:

- On Node A: `Delete("user:42")`
- On Node B: `Put("user:42", {"name": "Alice"})` (concurrently, hasn't seen the delete)

What now? And — much subtler — what if Node A's delete propagates everywhere,
every node forgets the key, and *then* a straggler node that was offline during
all this reconnects, still holding its old copy of `user:42`? It must **not** be
allowed to resurrect the deleted key. Deletes are the villains of distributed
data; chapter 10 is devoted to them.

A **CRDT** (Conflict-free Replicated Data Type) is a data structure designed so
that all of these merges have a single, automatic, order-independent answer.

## 2.2 The three laws that make convergence work

A merge function `merge(x, y)` that combines two replica states must obey three
algebraic laws. If it does, **strong eventual consistency** comes for free:

1. **Commutative:** `merge(a, b) = merge(b, a)`. Order of two messages doesn't
   matter.
2. **Associative:** `merge(merge(a, b), c) = merge(a, merge(b, c))`. Grouping
   doesn't matter.
3. **Idempotent:** `merge(a, a) = a`. Receiving the same message twice is
   harmless.

Why these three? In a real network, messages arrive **out of order** (need
commutativity), get **batched differently** on different paths (need
associativity), and get **redelivered** by retries (need idempotency). A function
with all three laws is exactly a **join** (least upper bound) on a mathematical
structure called a **join-semilattice**. The practical upshot: *every replica
that has seen the same set of updates is in the same state, period* — there is no
need to reason about timing at all. convergeKV's `Document.Merge` is such a join,
and these three laws are property-tested directly
(`internal/crdt/property_test.go`).

## 2.3 Building blocks: dots, version vectors, causal contexts

To make deletes safe, a replica must remember not just *what values it has* but
*what events it has ever witnessed*. That calls for a compact way to name events
and to record sets of them.

### Dots — naming a single write event

A **dot** is a globally unique label for one write event:

```go
// internal/crdt/types.go
type Dot struct {
    Actor ActorID  // [16]byte — which replica created this event
    Seq   uint64   // that replica's per-document counter: 1, 2, 3, ...
}
```

Read it as "the `Seq`-th event minted by `Actor`." Because each actor only ever
increments its own counter, and actors have distinct IDs, **no two writes
anywhere ever share a dot.** A dot is like a globally-unique receipt number for a
single change.

> **Subtle but crucial detail — per-document sequence numbers.** In convergeKV the
> `Seq` counter is *per document*, not global to the actor. The next dot for a
> document is `doc.Context.VV[self] + 1` (`coordinator.minter`,
> `internal/coordinator/coordinator.go:387`). Why per-document? Because (a)
> uniqueness only needs to hold *within one document's context* for the merge math
> to work, and (b) it keeps the causal context compact — see §2.7. A node-wide
> counter instead suffers unbounded context growth, so the code mints per
> document.

### Version vectors — remembering a contiguous run of events

If an actor's events for a document arrive in order — dot 1, then 2, then 3 —
there is no need to store the set `{1, 2, 3}` explicitly; that grows forever.
Instead the system stores the single number `3` and *means* "events 1 through 3
have all been seen." That compact form is a **version vector** (VV): a map from
each actor to the highest *contiguous* sequence number seen from it.

```go
// internal/crdt/context.go
type CausalContext struct {
    VV    map[ActorID]uint64   // VV[a] = n  means "seen a's dots 1..n, all of them"
    Cloud map[Dot]struct{}     // out-of-order dots not yet contiguous
}
```

`VV[actorA] = 5` is shorthand for "actorA's events 1, 2, 3, 4, *and* 5 have all
been observed." That is five facts stored as one number. This works only because
the run is contiguous.

### The cloud — events that arrived with a gap

Networks deliver out of order. Suppose actorA's events 1, 2, 3 have arrived, and
then — skipping 4 — event 5 arrives. `VV[actorA]` cannot be bumped to 5, because
that would falsely claim event 4 had been seen. So 5 goes into the **cloud**, a
set of individual dots that are not yet part of any contiguous run:

```
VV[actorA] = 3
Cloud = { (actorA, 5) }
```

When the missing event 4 finally arrives, **compaction** kicks in: 4 makes the
run contiguous up to 5, so both fold into the VV:

```
VV[actorA] = 5
Cloud = { }
```

This is exactly `CausalContext.Add` and `compactActor`
(`internal/crdt/context.go:29`). The cloud is a temporary holding pen; in healthy
operation it stays small and drains as gaps fill.

### Causal context — the union of VV and cloud

Together, the **causal context** answers one question for any dot: *"Has this
event ever been seen?"*

```go
// internal/crdt/context.go:19
func (c *CausalContext) Contains(d Dot) bool {
    if d.Seq <= c.VV[d.Actor] {   // inside a contiguous run
        return true
    }
    _, ok := c.Cloud[d]           // or sitting in the cloud
    return ok
}
```

`Contains` is worth remembering — it is the single most consulted predicate in
the merge, and it is how deletes are made permanent.

## 2.4 The document: an OR-Map of registers

Now the value itself. A convergeKV document for one key is a **map of top-level
fields**, plus the causal context:

```go
// internal/crdt/document.go
type Document struct {
    Fields  map[string][]Register  // field name -> set of concurrent values
    Context CausalContext
}

// internal/crdt/types.go
type Register struct {
    Dot   Dot     // the write event that produced this value
    HLC   HLC     // timestamp, for last-writer-wins (chapter 3)
    Value []byte  // the field's value as opaque bytes (a JSON scalar/array/object)
}
```

Two design choices to absorb:

**1. Merge happens at the top level only.** The JSON value `{"name": "Alice",
"addr": {"city": "Genoa"}}` becomes two fields: `name` and `addr`. Each top-level
field is an independent unit that merges separately. But `addr`'s value — the
whole nested object `{"city": "Genoa"}` — is stored as **opaque bytes** and
treated as one indivisible value. convergeKV does *not* merge inside nested
objects; if two clients concurrently set `addr`, one whole `addr` object wins, not
a field-by-field blend. This keeps the model simple and is handled at the JSON
boundary in `internal/codec/json.go` (`SplitFields` / `RenderDocument`).

This structure — a map where keys can be added and removed concurrently — is
called an **OR-Map** (Observed-Remove Map). Each field is an **OR** element: it is
"present" if some value-dot for it exists that hasn't been observed-removed.

**2. Each field holds a *set* of registers, not one.** Notice `Fields` maps to
`[]Register`, a slice. Why would a field have multiple values at once? Because two
replicas might concurrently write the same field before they sync. Rather than
collapse them to a winner *at merge time* (which, surprisingly, breaks
convergence — see §2.8), convergeKV **keeps every concurrent register** and
resolves the winner only when a client *reads*:

```go
// internal/crdt/document.go:43 — read-time last-writer-wins
func (d *Document) Get(field string) (Register, bool) {
    regs := d.Fields[field]
    if len(regs) == 0 {
        return Register{}, false
    }
    win := regs[0]
    for _, r := range regs[1:] {
        if r.supersedes(win) {   // higher HLC, then actor bytes, then seq
            win = r
        }
    }
    return win, true
}
```

The client always sees exactly **one** value per field (no partial mixing), but
internally the document may carry several until the next write collapses them.
This is the "multi-value leaf" design, and it is the single most important
correctness decision in the codebase. §2.8 explains why the naive alternative is
wrong.

## 2.5 Local operations: how a write produces a delta

When the applier executes a client write, it does two things at once: it mutates
its own document, *and* it produces a **delta** — a tiny document describing just
this change — to ship to the other owners. Consider `Put`.

```go
// internal/crdt/document.go:82
func (d *Document) Put(field string, value []byte, dot Dot, ts HLC) *Document {
    d.ensure()
    delta := NewDocument()
    for _, old := range d.Fields[field] {
        delta.Context.Add(old.Dot)      // (A) record the dots being replaced
    }
    reg := Register{Dot: dot, HLC: ts, Value: value}
    d.Fields[field] = []Register{reg}   // (B) new value replaces the whole set
    d.Context.Add(dot)                  // (C) remember this new event locally

    delta.Fields[field] = []Register{reg}
    delta.Context.Add(dot)              // (D) delta carries new dot + replaced dots
    return delta
}
```

The non-obvious line is **(A)**. Why must the delta's context include the dots of
the *old* values being overwritten? Because that is how a peer learns to discard
those old values. When the delta reaches a peer, the peer sees "this delta's
context contains the old register's dot" and concludes "the writer had seen that
old value and chose to replace it — drop it here too." Without (A), a peer that
still held an old concurrent value could keep it forever. Property testing
surfaces exactly this kind of convergence bug.

**RemoveField** is similar but produces no new value — only context:

```go
// internal/crdt/document.go:116
func (d *Document) RemoveField(field string, dot Dot) *Document {
    delta := NewDocument()
    for _, old := range d.Fields[field] {
        delta.Context.Add(old.Dot)   // the removed value's dot — this IS the removal
    }
    delete(d.Fields, field)
    d.Context.Add(dot)               // the remove is itself an event, with its own dot
    delta.Context.Add(dot)
    return delta
}
```

A removal is communicated **entirely through the context**: the delta has no value
for the field, but its context contains the removed register's dot. A peer
receiving this reasons: "the local value for this field has a dot that the
sender's context contains — the sender observed that value and removed it — so
drop it." Absence of a field, *plus* a context that covers its dots, *means*
"removed." This is the defining trick of an OR-Map.

**Delete** (whole document) is the same idea over every field at once
(`document.go:131`): clear all fields, but keep a context covering every removed
dot plus the delete event. The local document becomes "empty fields, non-empty
context" — a **residual context**, which is the deletion's tombstone. Chapter 10
is all about when it is finally safe to throw that residual away.

> **One dot per field.** A client write touching three fields mints *three* dots,
> one per field, sharing one HLC (`PutMulti`, `document.go:100`). Fields are
> removed independently later, and removal is communicated per-dot, so each field
> needs its own dot.

## 2.6 The merge (the join) — line by line

Here is the operation that makes everything converge. When a replica receives a
delta (or a full document during anti-entropy), it folds it in:

```go
// internal/crdt/document.go:151
func (d *Document) Merge(remote *Document) {
    d.ensure()
    for f := range remote.Fields {
        if merged := mergeField(d.Fields[f], &d.Context, remote.Fields[f], &remote.Context); len(merged) > 0 {
            d.Fields[f] = merged
        } else {
            delete(d.Fields, f)
        }
    }
    for f, regs := range d.Fields {           // fields only local has
        if _, inRemote := remote.Fields[f]; inRemote {
            continue
        }
        if merged := mergeField(regs, &d.Context, nil, &remote.Context); len(merged) > 0 {
            d.Fields[f] = merged
        } else {
            delete(d.Fields, f)
        }
    }
    d.Context.UnionWith(remote.Context)       // contexts always union
}
```

The whole thing delegates per-field to `mergeField`, the **dotted-store join**:

```go
// internal/crdt/document.go:175
func mergeField(local []Register, localCtx *CausalContext,
                remote []Register, remoteCtx *CausalContext) []Register {
    out := make([]Register, 0, len(local)+len(remote))
    for _, l := range local {
        // keep a local register iff: the remote also has it (same dot),
        // OR the remote has never seen it (so can't have removed it)
        if hasDot(remote, l.Dot) || !remoteCtx.Contains(l.Dot) {
            out = append(out, l)
        }
    }
    for _, r := range remote {
        // adopt a remote register iff: it isn't already held locally,
        // AND it has never been seen (so it wasn't already removed)
        if !hasDot(local, r.Dot) && !localCtx.Contains(r.Dot) {
            out = append(out, r)
        }
    }
    // ...sort by dot for a canonical, deterministic order...
}
```

The rule for whether a register `r` survives the merge is a small, elegant piece
of logic. Read it as: **a value survives iff it is present on a side that has not
observed-and-removed it.**

| Situation | Local has `r`? | Remote has `r`? | Remote context contains `r.Dot`? | Outcome |
|-----------|:--:|:--:|:--:|---------|
| Both still hold it | yes | yes | — | **keep** (same write) |
| Local holds it, remote never saw it | yes | no | no | **keep** (remote can't have removed what it never saw) |
| Local holds it, remote saw & dropped it | yes | no | **yes** | **drop** (remote observed-removed it) |
| Remote holds it, local never saw it | no | yes | (local ctx: no) | **adopt** (new locally) |
| Remote holds it, local saw & dropped it | no | yes | (local ctx: yes) | **stay dropped** (it was removed) |

The "saw it" question is answered by the causal `Contains` check from §2.3. This
is the entire reason the causal context is dragged around: it lets a replica
distinguish *"a value never heard of"* (adopt it) from *"a value deliberately
removed"* (keep it removed). **A timestamp alone cannot tell those two apart** —
that is why naive last-writer-wins resurrects deleted data, and why convergeKV
uses causal contexts.

Finally, contexts always union (`UnionWith`, `context.go:60`): pointwise max of
the VVs, union of clouds, then compact. After merging, a replica's context covers
*everything either side had witnessed.* This is what makes removals stick: once a
dot is in a context, `Contains` returns true forever, so that value can never be
re-adopted by a future merge.

### Concurrent same-field writes

Notice the merge **never picks a winner** when two registers for one field are
genuinely concurrent — it just keeps both in the set. The winner is computed only
at read time (`Get`, §2.4) and only when persisting (the next `Put` overwrites the
whole set). This is intentional and is the subject of §2.8. The arbitration rule
itself, `supersedes`, is:

```go
// internal/crdt/types.go:34
func (r Register) supersedes(o Register) bool {
    if r.HLC != o.HLC { return r.HLC > o.HLC }                      // 1. higher HLC wins
    if c := bytes.Compare(r.Dot.Actor[:], o.Dot.Actor[:]); c != 0 { // 2. then actor bytes
        return c > 0
    }
    return r.Dot.Seq > o.Dot.Seq                                    // 3. then sequence
}
```

Three tiebreakers, applied in order, guarantee a **total order** — there is always
exactly one winner, and every replica computes the same one. The HLC (chapter 3)
is the meaningful "which happened later" signal; the actor and seq are
deterministic fallbacks so that even two writes with identical timestamps resolve
identically everywhere.

## 2.7 Why convergeKV converges — the intuition

The formal lattice proof is not needed to see why convergence is inevitable:

- **Contexts only grow.** `Contains` never goes from true back to false (within a
  document's life). Once an event has been witnessed, it stays witnessed.
- **A value's fate is decided by witness, not by timing.** "Keep" vs "drop"
  depends only on *which dots are in which context* — set membership, which is
  order-independent.
- **Reading is a pure function** of the multi-value document. Two byte-identical
  documents always render the same JSON.

So if two replicas have absorbed the same set of deltas — in any order, with any
duplicates — they hold the same fields, the same register sets, and the same
context. Byte-identical. That is strong eventual consistency, and the property
test `TestPropertyConvergence` hammers it with 10k randomized op sequences across
simulated replicas.

## 2.8 Why "collapse to a winner at merge time" is *wrong*

This is the subtle bug that justifies the whole multi-value-leaf design, and it is
worth understanding because it is a trap any reimplementation would fall into.

Suppose, instead of keeping concurrent registers, the merge collapsed them to a
single LWW winner *during merge*. Concretely: two replicas concurrently write
field `x` (registers `Rwin` with the higher HLC, and `Rlose`). A merge sees both,
keeps only `Rwin`, discards `Rlose`. This seems fine — the client only wants one
value anyway.

Here is the failure. The discarded `Rlose` had a dot, say `(B, 7)`. When `Rlose`
is thrown away at merge time, **where does the record of dot `(B, 7)` live?** Only
in the *origin* replica's full context. But **deltas don't carry the full
context** — a delta carries only the dots relevant to its change. So a third
replica that adopts `Rlose` *before* it ever learns the field was later removed
has dot `(B, 7)` sitting in a register but **not** in any context that a removal
delta would mention. The removal can never reach it. That replica keeps the stale
value forever: divergence that never heals.

The multi-value design dodges this entirely: every concurrent register is retained
through merge, so its dot is always physically present and accounted for; the
removal logic in §2.6 sees it and drops it correctly. A later `Put` collapses the
set on the write path (where the full local context *is* available), so the set
does not grow without bound either. Property testing finds exactly this
counterexample for the collapse-at-merge approach. The lesson: **in a delta-based
CRDT, a value's identity may not be discarded at a place that doesn't carry the
full context.**

## 2.9 Canonical encoding — turning a document into bytes

For storage, hashing, and the wire, a document must serialise to bytes — and for
two replicas to *recognise* they are identical, that serialisation must be
**canonical**: the same logical document always produces the exact same byte
string. That means deterministic ordering everywhere.

```go
// internal/crdt/encode.go — AppendCanonical
// fields sorted by name; within a field, registers sorted by dot;
// VV sorted by actor; cloud sorted by (actor, seq); all integers big-endian.
```

Canonical encoding matters for two reasons:

1. **Merkle hashing** (chapter 8) hashes the canonical document. If encoding
   weren't deterministic, two equal documents could hash differently and
   anti-entropy would chase phantom differences forever.
2. **Idempotent merge detection.** `MergeDelta` compares `before` and `after`
   canonical bytes to notice "this delta changed nothing" and skip a needless disk
   write (`coordinator.go:289`).

`DecodeDocument` (`internal/crdt/decode.go`) is the strict inverse: it rejects
truncated input, trailing bytes, and empty register sets, because the canonical
form is also the on-disk storage format and corruption must be caught, not
silently tolerated.

## 2.10 Summary

- A **dot** `(actor, seq)` uniquely names one write event.
- A **causal context** (version vector + cloud) compactly records *every event a
  replica has witnessed*, and answers `Contains(dot)`.
- A **document** is an OR-Map of fields; each field holds a *set* of concurrent
  **registers**; the client reads the single **LWW** winner.
- Local ops (`Put`/`RemoveField`/`Delete`) mutate the document *and* emit a
  **delta** whose context carries exactly the dots needed to communicate the
  change (including replaced/removed dots). The client API surfaces these as
  `Put` (replace — set fields, `RemoveField` the omitted ones), `Patch` (set
  fields, `RemoveField` the named ones), and `Delete` (whole document); see
  [chapter 7](07-request-paths.md).
- **Merge** is a dotted-store join: a register survives iff it's present on a side
  that hasn't observed-removed it; contexts always union. The result is
  commutative, associative, idempotent → **strong eventual consistency**.
- Concurrent registers are kept until a write collapses them, because discarding a
  value's dot at merge time would let deletes fail to propagate.

Next: [time and the hybrid logical clock](03-clocks-hlc.md) — what the `HLC` field
in a register actually is, and why wall-clock time isn't enough.
