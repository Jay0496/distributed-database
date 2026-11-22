# Architecture

This document describes the system decomposition, deployment topology, and the
mapping to the course's **functional separation**, **transparency**, and
**heterogeneity** principles.

## System context

```mermaid
flowchart LR
    Client["Client application<br/>(CLI or gRPC)"]
    Gateway["Gateway<br/>leader discovery + retry<br/>single logical endpoint"]
    subgraph Cluster["5-node cluster (Docker Compose)"]
        N1["node1<br/>:50051"]
        N2["node2<br/>:50052"]
        N3["node3<br/>:50053"]
        N4["node4<br/>:50054"]
        N5["node5<br/>:50055"]
    end
    Client -->|"put / get / status / mode"| Gateway
    Gateway -->|"gRPC to Raft leader"| N1
    Gateway -.->|"redirect on failover"| N2
    N1 <-->|"Raft + shards + heartbeat"| N2
    N2 <-->|"Raft + shards + heartbeat"| N3
    N3 <-->|"Raft + shards + heartbeat"| N4
    N4 <-->|"Raft + shards + heartbeat"| N5
    N5 <-->|"Raft + shards + heartbeat"| N1
```

## Docker deployment topology

```mermaid
flowchart TB
    subgraph compose["docker compose network"]
        subgraph nodes["node services (always on)"]
            node1["node1<br/>python -m node.main<br/>port 50051"]
            node2["node2<br/>port 50052"]
            node3["node3<br/>port 50053"]
            node4["node4<br/>port 50054"]
            node5["node5<br/>port 50055"]
        end
        gateway["gateway<br/>profile: tools<br/>CLI container"]
        testnode["node<br/>profile: test<br/>pytest"]
    end
    v1[("node1_data")]
    v2[("node2_data")]
    v3[("node3_data")]
    v4[("node4_data")]
    v5[("node5_data")]
    node1 --- v1
    node2 --- v2
    node3 --- v3
    node4 --- v4
    node5 --- v5
    gateway -.->|"PEERS env"| nodes
```

## Module map

| Layer | Package / file | Responsibility |
|---|---|---|
| Client entry | `gateway/main.py` | CLI parsing, JSON output |
| Client logic | `gateway/client.py` | Leader discovery, retry, put/get/status/mode |
| Routing | `gateway/ring.py` | Consistent-hash ring (150 vnodes/node) |
| RPC contract | `proto/kv.proto`, `gen/` | Protobuf messages + generated stubs |
| Node service | `node/service.py` | `NodeService` implements the KV RPCs |
| Auth | `node/auth.py` | `AuthInterceptor` shared-token check |
| Transport | `node/transport.py` | `GrpcTransport` peer RPCs with token metadata |
| Consensus facade | `node/raft/node.py` | `RaftNode` composes the mixins |
| Election | `node/raft/election.py` | `RaftElection` |
| Replication | `node/raft/replication.py` | `RaftReplication` |
| Persistence/FSM | `node/raft/persistence.py` | `RaftPersistence` |
| Failure detection | `node/raft/failure_detector.py` | `RaftFailureDetector` |
| Redundancy | `node/encode.py` | `RS32Engine`, `ReplicationEngine`, `checksum` |
| Repair | `node/repair.py` | `read_repair`, `RepairSweeper`, `sync_on_start` |
| Storage | `node/storage.py` | `Storage` over SQLite (WAL) |
| Logging | `node/logging.py` | Structured JSON, redaction |
| Startup | `node/main.py` | Wire storage + raft + server + sweeper + sync |

## Why this satisfies the principles

- **Functional separation** — each concern has its own module/class (see table).
- **Transparency** — only the gateway is exposed to clients; topology is hidden.
- **Heterogeneity** — gRPC/Protobuf over HTTP/2; language-agnostic contract.

## Where the code lives

- Compose services/ports/volumes: `docker-compose.yml`
- Server bootstrap: `node/main.py::main`
- gRPC server assembly: `node/server.py::serve`
- Service implementation: `node/service.py::NodeService`

## Verified by

- `tests/test_node_grpc.py::test_store_fetch_has_roundtrip`
- `docker compose ps` → all five nodes `(healthy)`
- `scripts/smoke.sh` → end-to-end cluster smoke
