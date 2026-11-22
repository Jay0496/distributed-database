import os
import sys
import time

try:
    from node.encode import RS32Engine
    from node.repair import RepairSweeper
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from node.encode import RS32Engine  # type: ignore
    from node.repair import RepairSweeper  # type: ignore
    from node.storage import Storage  # type: ignore


class FakeTransport:
    def __init__(self, shards):
        self.shards = dict(shards or {})
        self.store_calls = []

    def fetch_shard(self, owner, key, idx):
        return self.shards.get((owner, idx), None)

    def store_shard(self, owner, key, idx, version, data):
        self.store_calls.append((owner, key, idx, version, bytes(data)))
        self.shards[(owner, idx)] = bytes(data)
        return True


def test_sweep_refills_missing_shard(tmp_path):
    owners = [f"node{i}" for i in range(5)]
    key = "k"
    version = 3
    value = b"secret payload"
    canonical = RS32Engine().encode(value)

    storage = Storage(str(tmp_path / "s.db"))
    storage.upsert_meta(key, "rs32", version, len(value), "checksum", owners)

    shards = {(owners[i], i): canonical[i] for i in range(5) if i != 2}
    transport = FakeTransport(shards)

    sweeper = RepairSweeper("node0", storage, transport, interval=0.01)
    sweeper.sweep_once()

    assert transport.store_calls == [(owners[2], key, 2, version, canonical[2])]

    storage.close()


def test_sweep_does_not_touch_healthy_shards(tmp_path):
    owners = [f"node{i}" for i in range(5)]
    key = "k"
    version = 3
    value = b"secret payload"
    canonical = RS32Engine().encode(value)

    storage = Storage(str(tmp_path / "s2.db"))
    storage.upsert_meta(key, "rs32", version, len(value), "checksum", owners)

    shards = {(owners[i], i): canonical[i] for i in range(5)}
    transport = FakeTransport(shards)

    sweeper = RepairSweeper("node0", storage, transport, interval=0.01)
    sweeper.sweep_once()

    assert transport.store_calls == []

    storage.close()


def test_sweep_run_stops_on_flag():
    import threading

    class EmptyStorage:
        def list_meta_keys(self):
            return []

        def get_meta(self, key):
            return None

    stop = threading.Event()
    sweeper = RepairSweeper("node0", EmptyStorage(), FakeTransport({}), interval=0.01, stop_event=stop)
    t = threading.Thread(target=sweeper.run, daemon=True)
    t.start()
    time.sleep(0.1)
    stop.set()
    t.join(timeout=1.0)
    assert not t.is_alive()
