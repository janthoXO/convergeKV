# Benchmark report (M9)

Reproduce with:

```sh
CONVERGEKV_BENCH=1 go test ./test/cluster/ -run TestBenchmarkReport -v
```

## Environment

- 5-node in-process cluster (test harness), 16 partitions, RF=3
- 1KB JSON documents, single client, sequential requests
- Apple Silicon (darwin/arm64), Go 1.26, Pebble with synced commits
- 2000 samples for put/get, 100 for convergence

## Results (2026-06-13, commit at M9)

| Operation | p50 | p99 | max |
|---|---|---|---|
| Put (applier apply + synced persist) | 12.0 ms | 21.1 ms | 29.1 ms |
| Get (single active owner, local read) | 59 µs | 205 µs | 4.1 ms |
| Convergence (write visible byte-equal on all 3 owners) | 29.5 ms | 42.2 ms | 42.2 ms |

## Notes

- Put latency is dominated by the synced Pebble batch commit (`pebble.Sync`
  per applied write — "persisted locally" is part of the write contract,
  spec §1). Group-commit batching across concurrent writers is the obvious
  future optimization; sequential single-client puts pay one fsync each.
- Convergence latency covers the asynchronous delta fan-out plus the synced
  apply on the two replica owners; it is bounded by fan-out delivery, not by
  anti-entropy (AE only repairs what fan-out drops).
- Reads never touch other nodes when the receiving node is an active owner;
  the µs-scale path is one Pebble point read plus JSON rendering.
- Allocation tuning (sync.Pool etc.) was not applied: profiles do not show
  hot-path allocation pressure at these throughputs (spec M9 says optimize
  only after profiles justify it).
