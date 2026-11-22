import os
import sys
import time

try:
    import gen.kv_pb2 as kv_pb2
    from node.raft import DirectTransport, RaftNode
    from node.server import NodeService
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from server import NodeService  # type: ignore

    import gen.kv_pb2 as kv_pb2  # type: ignore
    from raft import DirectTransport, RaftNode  # type: ignore
    from storage import Storage  # type: ignore


def _make_cluster(tmp_path):
    ids = [f"node{i}" for i in range(3)]
    storages = {}
    nodes = {}
    services = {}
    for nid in ids:
        st = Storage(str(tmp_path / f"{nid}.db"))
        storages[nid] = st
        raft = RaftNode(nid, [x for x in ids if x != nid], storage=st)
        nodes[nid] = raft
        services[nid] = NodeService(nid, st, raft)
    transport = DirectTransport(nodes)
    for nd in nodes.values():
        nd.transport = transport
    for nd in nodes.values():
        nd.start()
    deadline = time.monotonic() + 5.0
    leader_id = None
    while time.monotonic() < deadline:
        for nid, nd in nodes.items():
            if nd.is_leader():
                leader_id = nid
                break
        if leader_id:
            break
        time.sleep(0.05)
    assert leader_id is not None, "no leader elected"
    time.sleep(0.3)
    for nd in nodes.values():
        nd.failure_heartbeat_interval = 0.1
        nd.failure_threshold = 0.3
        nd.degraded_after = 0.3
    return ids, nodes, storages, services, leader_id, transport


def _teardown(nodes, storages):
    for nd in nodes.values():
        try:
            nd.stop()
        except Exception:
            pass
    for st in storages.values():
        try:
            st.close()
        except Exception:
            pass


def test_degraded_on_peer_failure(tmp_path):
    ids, nodes, storages, services, leader_id, transport = _make_cluster(tmp_path)
    try:
        target = None
        for nid in ids:
            if nid != leader_id:
                target = nid
                break
        assert target is not None
        orig = transport.heartbeat

        def failing(peer_id, *a, **k):
            if peer_id == target:
                raise RuntimeError("boom")
            return orig(peer_id, *a, **k)

        transport.heartbeat = failing
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target not in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target in nodes[leader_id].degraded
        # GetLeader surfaces degraded
        resp = services[leader_id].GetLeader(kv_pb2.GetLeaderRequest(), None)
        assert target in list(resp.degraded)

        # recovery
        transport.heartbeat = orig
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target not in nodes[leader_id].degraded
    finally:
        _teardown(nodes, storages)


def test_node_stop_marks_degraded(tmp_path):
    ids, nodes, storages, services, leader_id, transport = _make_cluster(tmp_path)
    try:
        target = None
        for nid in ids:
            if nid != leader_id:
                target = nid
                break
        assert target is not None
        nodes[target].stop()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target not in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target in nodes[leader_id].degraded

        # recovery after restart
        nodes[target].start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and target in nodes[leader_id].degraded:
            time.sleep(0.05)
        assert target not in nodes[leader_id].degraded
    finally:
        _teardown(nodes, storages)
