# Routing & Consistent Hashing

Maps to the proposal's **Client API Gateway & Router Module** and the course's
**migration/load balancing** and **transparency** principles.

## Two-layer routing

1. **Gateway → leader (actual client traffic).** The gateway discovers the Raft
   leader and sends `Put`/`Get` directly to it, retrying on failover.
2. **Consistent-hash ring (routing/migration demo).** A 150-vnode ring maps
   keys to physical nodes. This is retained for the rubric's
   migration/load-balancing bullet and for demonstrating minimal remap; it does
   not select the entry node for data traffic.

## Ring layout

```mermaid
flowchart LR
    K1["key-abc"] --> H["md5(key) → 128-bit position"]
    H --> B["bisect_left on sorted vnode positions"]
    B --> V{"first vnode ≥ position?<br/>else wrap to 0"}
    V --> N["physical node id"]
    subgraph Ring["150 vnodes per physical node"]
        direction LR
        v1["node1:0 … node1:149"]
        v2["node2:0 … node2:149"]
        v3["node3:0 … node3:149"]
        v4["node4:0 … node4:149"]
        v5["node5:0 … node5:149"]
    end
    N --- Ring
```

## Minimal remap on membership change

```mermaid
flowchart TD
    A["5 nodes, 1000 keys mapped"] --> B["remove node0"]
    B --> C{"key previously owned by node0?"}
    C -->|yes| D["reassign clockwise to next vnode owner"]
    C -->|no| E["unchanged"]
    D --> F["only node0's keys move"]
    E --> F
    F --> G["re-add node0 (same deterministic vnode positions)"]
    G --> H["original mapping restored"]
```

## Where the code lives

- Ring: `gateway/ring.py::ConsistentHashRing` (`__init__` 150 vnodes,
  `add_node`, `remove_node`, `get_node`, `nodes`, `__len__`)
- Leader discovery/retry: `gateway/client.py::GatewayClient.discover_leader`,
  `get_leader_stub`, `call_with_retry`
- Leader id → address mapping: `gateway/client.py::_map_leader_to_addr`

## Distribution expectation

With 150 vnodes/node and MD5, each of 5 nodes receives roughly 20% of keys.
The test asserts 15–25% per node.

## Verified by

- `tests/test_ring.py::test_distribution_across_nodes`
- `tests/test_ring.py::test_minimal_remap_on_remove`
- `tests/test_ring.py::test_minimal_remap_any_victim`
- `tests/test_ring.py::test_readd_restores_mapping`
- `tests/test_gateway_leader.py::test_leader_failover_rediscovery`
