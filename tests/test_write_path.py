import os
import sys
import time

try:
    import gen.kv_pb2 as kv_pb2
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import gen.kv_pb2 as kv_pb2  # type: ignore


def test_put_then_inspect_shards(cluster):
    ids, nodes, storages, services, lid, transport = cluster
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
        pass


def test_put_fails_cleanly_if_node_down(cluster):
    ids, nodes, storages, services, lid, transport = cluster
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
        pass