# Read Path (GET)

Maps to the proposal's **Redundancy & Encoding Engine** (reconstruction) and the
course's **fault tolerance** principle.

## Sequence

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (gateway CLI)
    participant G as GatewayClient
    participant L as Leader (NodeService)
    participant S as Storage (SQLite)
    participant N1 as node1
    participant N2 as node2
    participant N3 as node3
    participant N4 as node4
    participant N5 as node5
    participant E as RedundancyEngine
    participant R as ReadRepair

    C->>G: get(key)
    G->>G: discover_leader() / get_leader_stub()
    G->>L: Get(GetRequest)
    L->>S: get_meta(key)
    alt metadata missing
        S-->>L: None
        L-->>G: GetResponse(found=false, error="not found")
    else metadata present
        S-->>L: mode, version, size, checksum, owners
        par FetchShard (parallel, 2s timeout)
            L->>N1: FetchShard(key, 0)
            L->>N2: FetchShard(key, 1)
            L->>N3: FetchShard(key, 2)
            L->>N4: FetchShard(key, 3)
            L->>N5: FetchShard(key, 4)
        end
        N1-->>L: shard bytes or None
        N2-->>L: shard bytes or None
        N3-->>L: shard bytes or None
        L->>E: decode([D1,D2,D3,None,None])
        E-->>L: original bytes
        L->>L: verify len == size
        L->>L: verify SHA-256 == checksum
        L->>R: read_repair(shards, canonical)
        R-->>L: schedule StoreShard for missing/corrupt
        L-->>G: GetResponse(value, found=true, version)
    end
    G-->>C: {"found": true, "value": "..."}
```

## Steps in code

| Step | Code |
|---|---|
| Leader lookup | `gateway/client.py::GatewayClient.get` → `call_with_retry` |
| RPC entry | `node/service.py::NodeService.Get` |
| Metadata lookup | `node/storage.py::Storage.get_meta` |
| Parallel fetch | `NodeService.Get::fetch_one` + `ThreadPoolExecutor` (2s) |
| Local fetch | `node/storage.py::Storage.fetch_shard` |
| Remote fetch | `node/transport.py::GrpcTransport.fetch_shard` |
| Decode | `node/encode.py::RS32Engine.decode` / `ReplicationEngine.decode` |
| Size check | `NodeService.Get` (`len(decoded) == size`) |
| Checksum check | `NodeService.Get` (`hashlib.sha256`) |
| Read repair | `node/repair.py::read_repair` (background `StoreShard`) |

## Reconstruction guarantees

- **RS(3,2):** any 3 of 5 shards reconstruct; up to 2 missing.
- **Replication:** any 1 of 5 copies reconstructs; up to 4 missing.
- Corrupt (same-length) RS shards are corrected by Reed-Solomon error
  correction (1 error) or treated as erasures when length differs.

## Read repair

After a successful decode, canonical shards are recomputed with
`engine.encode(decoded)` and compared with the fetched shards. Mismatched or
missing shards are re-`StoreShard`ed to their owner in a daemon thread; a log
line `read-repair: rebuilt shard i` is emitted.

## Verified by

- `tests/test_read_path.py::test_put_get_roundtrip[rs32]`
- `tests/test_read_path.py::test_put_get_roundtrip[replication]`
- `tests/test_read_path.py::test_corrupt_shard_reconstructs`
- `tests/test_read_path.py::test_missing_metadata_not_found`
- `tests/test_repair.py::test_read_repair_restores_corrupt_shard`
- `tests/test_repair.py::test_read_repair_log_line`
