# Failure Management & Recovery

Maps to the proposal's **Failure Management & Recovery Subsystem** and the
course's **fault tolerance**, **reliability**, and **migration/load balancing**
principles.

Three cooperating mechanisms:

1. **Failure detection** — leader heartbeats peers, marks degraded after ~3s.
2. **Read repair** — GET rebuilds missing/corrupt shards it had to reconstruct.
3. **Repair sweep + startup sync** — periodic refill and boot-time catch-up.

## Failure detection

```mermaid
sequenceDiagram
    autonumber
    participant L as Leader
    participant P as Peer
    participant S as StatusAPI
    participant G as Gateway

    loop every 1s while leader
        L->>P: Heartbeat node_id and term
        alt peer responds
            P-->>L: HeartbeatResponse ok
            L->>L: record last_seen and clear degraded
        else timeout or error
            L->>L: no update
        end
    end
    Note over L: no response for 3 seconds
    L->>L: add peer to degraded set
    G->>S: GetLeader
    S-->>G: degraded node3
    Note over P: peer restarts
    L->>P: Heartbeat
    P-->>L: ok
    L->>L: remove node3 from degraded set
```

Code: `node/raft/failure_detector.py::RaftFailureDetector._detector_loop`,
`mark_degraded`, `clear_degraded`, `_resolve_peer_id`.
Exposure: `node/service.py::NodeService.GetLeader`, `Heartbeat`.

## Read repair

```mermaid
flowchart TD
    A["GET decoded successfully"] --> B["canonical = engine.encode(decoded)"]
    B --> C{"for each shard i"}
    C --> D{"fetched[i] == canonical[i]?"}
    D -->|yes| C
    D -->|no or missing| E["log read-repair: rebuilt shard i"]
    E --> F["background StoreShard to owner i"]
    F --> C
```

Code: `node/repair.py::read_repair`, called from
`node/service.py::NodeService.Get`.

## Repair sweep

```mermaid
flowchart TD
    A["timer every 10s (SWEEP_INTERVAL)"] --> B["storage.list_meta_keys"]
    B --> C{"for each key"}
    C --> D["transport.fetch_shard from each owner"]
    D --> E{"all shards present?"}
    E -->|yes| C
    E -->|no| F["engine.decode then engine.encode"]
    F --> G{"for each shard i"}
    G --> H{"shards[i] != canonical[i]?"}
    H -->|no| G
    H -->|yes| I["StoreShard canonical[i] to owner"]
    I --> J["log sweep: refilled shard i on owner"]
    J --> G
```

Code: `node/repair.py::RepairSweeper.run`, `RepairSweeper.sweep_once`.
Started in `node/main.py::main` (daemon thread, `SWEEP_INTERVAL` env, default 10s).

## Startup sync

```mermaid
flowchart TD
    A["node boots"] --> B["Raft recovery / load_from_storage"]
    B --> C["sync_on_start"]
    C --> D["_collect_keys from peers ListKeys"]
    D --> E{"for each key"}
    E --> F{"node is an owner?"}
    F -->|no| E
    F -->|yes| G{"storage.has_shard?"}
    G -->|yes| E
    G -->|no| H["_fetch_shard from a healthy peer"]
    H --> I{"found?"}
    I -->|yes| J["storage.store_shard"]
    I -->|no| K["_reconstruct_missing: decode peers, encode, store"]
    J --> L["log sync pulled shard i for key"]
    K --> L
    L --> E
    E --> M["log sync pulled N shards"]
```

Code: `node/repair.py::sync_on_start`, `_collect_keys`, `_fetch_shard`,
`_reconstruct_missing`. Wired in `node/main.py::main` as a daemon thread after
Raft recovery.

## Recovery timeline (demo)

```mermaid
sequenceDiagram
    participant G as Gateway
    participant L as Leader
    participant N3 as node3
    Note over N3: docker compose stop node3
    L->>L: heartbeat misses, add node3 to degraded
    G->>L: get key
    L->>L: fetch 4 of 5 shards and reconstruct
    L-->>G: value, degraded read works
    Note over N3: docker compose start node3
    N3->>L: heartbeat and sync_on_start
    L->>L: remove node3 from degraded
    Note over L: sweep refills node3 shards within 10s
    G->>L: status
    L-->>G: degraded empty
```

## Known limitations

- No degraded writes: PUT requires all 5 shard acks.
- No snapshot compaction: log replay grows with history.
- Orphan shards from failed writes are not garbage-collected.
- Sweep is periodic (10s), so repair is not instantaneous.

## Verified by

- `tests/test_failure_detection.py::test_degraded_on_peer_failure`
- `tests/test_failure_detection.py::test_node_stop_marks_degraded`
- `tests/test_sweep.py::test_sweep_refills_missing_shard`
- `tests/test_sweep.py::test_sweep_does_not_touch_healthy_shards`
- `tests/test_sync.py::test_sync_on_start_pulls_missing_shard`
- `tests/test_sync.py::test_sync_on_start_reconstructs_missing_shard`
- `scripts/smoke.sh` (kill 1, kill 2, restart)
