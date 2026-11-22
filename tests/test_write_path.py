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
    # wait for leader
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


def test_put_then_inspect_shards(tmp_path):
    ids, nodes, storages, services, lid = _make_cluster(tmp_path)
    try:
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
        assert int(resp.version) == 1

        # each node holds exactly one shard for the key
        meta = storages[lid].get_meta("k")
        assert meta is not None
        owners = meta["owners"]
        for idx, owner in enumerate(owners):
            assert storages[owner].has_shard("k", idx) is True
        for nid in ids:
            cur = storages[nid]._conn.execute(
                "SELECT COUNT(*) FROM shards WHERE key = ?", ("k",)
            )
            assert cur.fetchone()[0] == 1

        meta = storages[lid].get_meta("k")
        assert meta is not None
        assert meta["mode"] == "rs32"
        assert meta["version"] == 1
        assert meta["size"] == 5
        assert meta["checksum"] == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        assert meta["owners"] == ids
        # metadata must not contain payload/value
        assert "value" not in meta
        assert "payload" not in meta
    finally:
        _teardown(nodes, storages)


def test_put_fails_cleanly_if_node_down(tmp_path):
    ids, nodes, storages, services, lid = _make_cluster(tmp_path)
    try:
        lid = ids[0]
        target = ids[1]
        transport = nodes[lid].transport
        orig = transport.store_shard

        def failing(peer_id, key, shard_index, version, data):
            if peer_id == target:
                return False
            return orig(peer_id, key, shard_index, version, data)

        transport.store_shard = failing

        svc = services[lid]
        resp = svc.Put(kv_pb2.PutRequest(key="kfail", value=b"hello", mode="rs32"), None)
        assert resp.ok is False
        # no metadata committed
        assert storages[lid].get_meta("kfail") is None
    finally:
        _teardown(nodes, storages)