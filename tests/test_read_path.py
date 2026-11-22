import os
import sys
import time

import pytest

try:
    import gen.kv_pb2 as kv_pb2
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import gen.kv_pb2 as kv_pb2  # type: ignore


@pytest.mark.parametrize("mode", ["rs32", "replication"])
def test_put_get_roundtrip(cluster, mode):
    ids, nodes, storages, services, lid, transport = cluster
    try:
        for _ in range(20):
            cur_lid = next((nid for nid, nd in nodes.items() if nd.is_leader()), None)
            if cur_lid is None:
                time.sleep(0.2)
                continue
            lid = cur_lid
            svc = services[lid]
            resp = svc.Put(kv_pb2.PutRequest(key="k", value=b"hello world", mode=mode), None)
            if resp is not None and resp.ok:
                break
            time.sleep(0.2)
        assert resp is not None and resp.ok is True

        g = services[lid].Get(kv_pb2.GetRequest(key="k"), None)
        assert g.found is True
        assert g.value == b"hello world"
        assert int(g.version) == 1
        assert g.error == "" or g.error is None or g.error == ""
    finally:
        pass


def test_corrupt_shard_reconstructs(cluster):
    ids, nodes, storages, services, lid, transport = cluster
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
        # corrupt the shard bytes on the owning node
        bad = b"\x00" * storages[target].fetch_shard("k", idx).__len__()
        storages[target].store_shard("k", idx, meta["version"], bad)

        g = services[lid].Get(kv_pb2.GetRequest(key="k"), None)
        assert g.found is True
        assert g.value == b"secret payload"
    finally:
        pass


def test_missing_metadata_not_found(cluster):
    ids, nodes, storages, services, lid, transport = cluster
    try:
        lid = next((nid for nid, nd in nodes.items() if nd.is_leader()), lid)
        g = services[lid].Get(kv_pb2.GetRequest(key="nope"), None)
        assert g.found is False
        assert g.error == "not found"
    finally:
        pass