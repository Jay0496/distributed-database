import os
import sys

try:
    from gateway.ring import ConsistentHashRing
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway"))
    from ring import ConsistentHashRing  # type: ignore


def test_distribution_across_nodes():
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    counts = {f"node{i}": 0 for i in range(5)}
    for k in range(1000):
        counts[ring.get_node(f"key-{k}")] += 1
    for node, count in counts.items():
        assert 150 <= count <= 250, (node, count)


def test_minimal_remap_on_remove():
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    keys = [f"key-{i}" for i in range(1000)]
    before = {k: ring.get_node(k) for k in keys}
    victim = "node0"
    ring.remove_node(victim)
    after = {k: ring.get_node(k) for k in keys}
    for k in keys:
        if before[k] != victim:
            assert after[k] == before[k], k
    assert all(after[k] != victim for k in keys if before[k] == victim)


def test_readd_restores_mapping():
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    keys = [f"key-{i}" for i in range(1000)]
    before = {k: ring.get_node(k) for k in keys}
    victim = "node0"
    ring.remove_node(victim)
    ring.add_node(victim)
    after = {k: ring.get_node(k) for k in keys}
    for k in keys:
        if before[k] == victim:
            assert after[k] == victim, k
    assert after == before


def test_get_node_empty_raises():
    import pytest

    ring = ConsistentHashRing()
    with pytest.raises(ValueError):
        ring.get_node("any-key")


def test_len_vnodes():
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    assert len(ring) == 5 * 150