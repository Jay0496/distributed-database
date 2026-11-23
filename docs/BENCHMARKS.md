# Benchmarks

A minimal put/get harness lives in [`scripts/bench.py`](../scripts/bench.py).
It measures p50/p95 latency and throughput for both redundancy modes.

## Run

```sh
docker compose up -d
python scripts/bench.py --keys 50 --value-size 1024 --get-repeats 3
```

In-container (gateway profile):

```sh
docker compose run --rm -e PEERS="node1:50051,node2:50052,node3:50053,node4:50054,node5:50055" \
  gateway python scripts/bench.py --keys 50 --value-size 1024
```

## Sample results

Environment: local Docker Desktop (Windows), 5 node containers + ephemeral
gateway, 50 keys, 1 KiB values, 3 GETs per key. These are indicative, not
rigorous; run on the target hardware for report numbers.

| Operation | Completed | p50 (ms) | p95 (ms) | Throughput (ops/s) |
|---|---:|---:|---:|---:|
| put rs32 | 50 | 61.2 | 64.9 | 15.0 |
| get rs32 | 150 | 9.0 | 11.9 | 109.4 |
| put replication | 50 | 68.2 | 98.1 | 13.6 |
| get replication | 150 | 15.6 | 22.5 | 69.1 |

Larger value (256 B) run for comparison:

| Operation | Completed | p50 (ms) | p95 (ms) | Throughput (ops/s) |
|---|---:|---:|---:|---:|
| put rs32 | 20 | 67.5 | 71.5 | 12.6 |
| get rs32 | 40 | 5.2 | 6.1 | 188.8 |
| put replication | 20 | 65.8 | 68.2 | 15.0 |
| get replication | 40 | 5.3 | 12.8 | 155.1 |

## Interpretation

- Writes are latency-bound by the 5 parallel `StoreShard` RPCs plus Raft
  metadata commit; both modes are similar because both write one shard/copy to
  each of the 5 nodes.
- RS reads are usually faster than replication reads here because RS fetches
  smaller shards (3 data + 2 parity of ~1/3 payload each) while replication
  fetches full copies.
- Throughput is modest because the benchmark is sequential (one client, one
  request at a time). Concurrent clients would raise throughput; the gRPC
  servers are threaded.
- Python is slower than Go would be; this is the documented trade-off.

## Notes

- The harness creates keys prefixed `bench-<mode>-<runid>-` and does not delete
  them (no DELETE RPC in the MVP). Use `docker compose down -v` to reset.
- `PEERS` can be set via env or `--peers`.
- Results vary with host load, Docker Desktop networking, and log level.
