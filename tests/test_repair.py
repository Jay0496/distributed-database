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
    import gen.kv_pb2 as kv_pb2  # type: ignore
    from node.raft import DirectTransport, RaftNode  # type: ignore
    from node.server import NodeService  # type: ignore
    from node.storage import Storage  # type: ignore


def _make_cluster(tmp_path):
    ids = [f"node{i}" for i in range(5)]
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
    time.sleep(0.5)
    return ids, nodes, storages, services, leader_id


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


def test_read_repair_restores_corrupt_shard(tmp_path):
    ids, nodes, storages, services, lid = _make_cluster(tmp_path)
    try:
        svc = services[lid]
        resp = None
        for _ in range(20):
            cur_lid = next((nid for nid, nd in nodes.items() if nd.is_leader()), None)
            if cur_lid is None:
                time.sleep(0.2)
                continue
            lid = cur_lid
            svc = services[lid]
            resp = svc.Put(kv_pb2.PutRequest(key="k", value=b"secret payload", mode="rs32"), None)
            if resp is not None and resp.ok:
                break
            time.sleep(0.2)
        assert resp is not None and resp.ok is True

        meta = storages[lid].get_meta("k")
        owners = meta["owners"]
        target = owners[0]
        idx = 0
        bad = b"\xff" * storages[target].fetch_shard("k", idx).__len__()
        storages[target].store_shard("k", idx, meta["version"], bad)

        g = services[lid].Get(kv_pb2.GetRequest(key="k"), None)
        assert g.found is True
        assert g.value == b"secret payload"

        from node.encode import RS32Engine
        canonical = RS32Engine().encode(b"secret payload")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if storages[target].fetch_shard("k", idx) == canonical[idx]:
                break
            time.sleep(0.05)
        assert storages[target].fetch_shard("k", idx) == canonical[idx]
    finally:
        _teardown(nodes, storages)


def test_read_repair_log_line(tmp_path, capsys):
    ids, nodes, storages, services, lid = _make_cluster(tmp_path)
    try:
        svc = services[lid]
        resp = None
        for _ in range(20):
            cur_lid = next((nid for nid, nd in nodes.items() if nd.is_leader()), None)
            if cur_lid is None:
                time.sleep(0.2)
                continue
            lid = cur_lid
            svc = services[lid]
            resp = svc.Put(kv_pb2.PutRequest(key="k", value=b"hello", mode="rs32"), None)
            if resp is not None and resp.ok:
                break
            time.sleep(0.2)
        assert resp is not None and resp.ok is True

        meta = storages[lid].get_meta("k")
        owners = meta["owners"]
        target = owners[0]
        idx = 0
        bad = b"\xff" * storages[target].fetch_shard("k", idx).__len__()
        storages[target].store_shard("k", idx, meta["version"], bad)

        g = services[lid].Get(kv_pb2.GetRequest(key="k"), None)
        assert g.found is True
        time.sleep(0.5)
        captured = capsys.readouterr()
        assert "read-repair: rebuilt shard 0" in captured.out
    finally:
        _teardown(nodes, storages)
