import pytest

from gateway.ring import ConsistentHashRing


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
    assert all(after[k] != victim for k in keys)


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
    assert after == before


def test_get_node_empty_raises():
    ring = ConsistentHashRing()
    with pytest.raises(ValueError):
        ring.get_node("any-key")


def test_len_vnodes():
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    assert len(ring) == 5 * 150


@pytest.mark.parametrize("victim", [f"node{i}" for i in range(5)])
def test_minimal_remap_any_victim(victim):
    ring = ConsistentHashRing()
    for i in range(5):
        ring.add_node(f"node{i}")
    keys = [f"key-{i}" for i in range(500)]
    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node(victim)
    after = {k: ring.get_node(k) for k in keys}
    for k in keys:
        if before[k] != victim:
            assert after[k] == before[k], (k, before[k], after[k])
