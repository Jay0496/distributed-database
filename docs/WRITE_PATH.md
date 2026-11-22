# Write Path (PUT)

Maps to the proposal's **Redundancy & Encoding Engine**, **Raft Consensus
Layer**, and the end-to-end PUT flow.

## Sequence

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (gateway CLI)
    participant G as GatewayClient
    participant L as Leader (NodeService)
    participant E as RedundancyEngine
    participant N1 as node1
    participant N2 as node2
    participant N3 as node3
    participant N4 as node4
    participant N5 as node5

    C->>G: put(key, value, mode?)
    G->>G: discover_leader() / get_leader_stub()
    G->>L: Put(PutRequest)
    L->>L: mode = request.mode or default_mode
    L->>L: version = previous_version + 1
    L->>L: checksum = SHA-256(value)
    L->>E: encode(value)
    E-->>L: 5 shards [D1,D2,D3,P1,P2] or 5 copies
    L->>N1: StoreShard(key, idx=0, version, shard[0])
    L->>N2: StoreShard(key, idx=1, version, shard[1])
    L->>N3: StoreShard(key, idx=2, version, shard[2])
    L->>N4: StoreShard(key, idx=3, version, shard[3])
    L->>N5: StoreShard(key, idx=4, version, shard[4])
    Note over L,N5: all five must ack (no partial commit)
    L->>L: raft.append_command(meta JSON)
    L->>L: wait until commit_index >= idx (3s timeout)
    L-->>G: PutResponse(ok=true, version)
    G-->>C: {"ok": true, "version": n}
```

## Steps in code

| Step | Code |
|---|---|
| Discover leader | `gateway/client.py::GatewayClient.discover_leader`, `get_leader_stub` |
| Retry on failure | `gateway/client.py::GatewayClient.call_with_retry` |
| RPC entry | `node/service.py::NodeService.Put` |
| Mode selection | `NodeService.Put` (`request.mode` or `storage.get_raft_state("default_mode")`) |
| Version bump | `NodeService.Put` (reads `storage.get_meta(key)`) |
| Encode | `node/encode.py::RS32Engine.encode` / `ReplicationEngine.encode` |
| Parallel shard write | `NodeService.Put::_store_one` + `ThreadPoolExecutor` |
| Local shard | `node/storage.py::Storage.store_shard` |
| Remote shard | `node/transport.py::GrpcTransport.store_shard` |
| Raft append | `node/raft/replication.py::RaftReplication.append_command` |
| Commit wait | `NodeService.Put` (`commit_index >= idx`) |
| FSM apply | `node/raft/persistence.py::RaftPersistence._default_apply` (`op=put` → `upsert_meta`) |

## Failure behaviour

- If any `StoreShard` fails (node down), `Put` returns
  `{"ok": false, "error": "store shard failed"}` and **does not append** the
  Raft metadata entry → no partial commit.
- If the Raft commit does not advance within 3s, returns
  `{"ok": false, "error": "commit timeout"}`.
- Orphan shards from a failed write may remain locally; cleanup is out of MVP
  scope (documented limitation).

## Metadata stored (not payload)

`meta` row: `key, mode, version, size, checksum, owners`. Shard bytes live only
in the `shards` table on each node.

## Verified by

- `tests/test_write_path.py::test_put_then_inspect_shards`
- `tests/test_write_path.py::test_put_fails_cleanly_if_node_down`
- `tests/test_node_grpc.py::test_store_fetch_has_roundtrip`
- `scripts/smoke.sh` (put/get round-trip)
