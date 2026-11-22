import os
import sys
import time

import pytest

try:
    import gen.kv_pb2 as kv_pb2
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import gen.kv_pb2 as kv_pb2  # type: ignore


def test_read_repair_restores_corrupt_shard(cluster):
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
        pass


def test_read_repair_log_line(cluster, capsys):
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
        pass